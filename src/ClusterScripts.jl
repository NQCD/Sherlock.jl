module ClusterScripts

# Write your package code here.
using Distributed
using DiffEqBase
using JLD2
using RobustPmap
using Glob
using ProgressBars

struct SimulationFile
    path::String
    stem::String
    name::String
    with_extension::String
    function SimulationFile(full_path::String;path_delim="/")
        split_path=split(full_path, path_delim)
        stem=join(split_path[1:end-1], path_delim)*path_delim
        with_extension=split_path[end]
        name=join(split(with_extension, ".")[1:end-1], ".")
        new(full_path, stem, name, with_extension)
    end
end

"""
    pmap_queue(target_function::Function, input_list::Vector{Dict}; trajectories_key="trajectories", ensemble_key="ensemble_algorithm", tmp_dir="tmp/", checkpoint_frequency=0, sort_variable="")
    Splits a list of input parameters into smaller operations for better multiprocessing, then runs the target function on the list of inputs and merges the results back together again. 

    Arguments similar to pmap: loadbalance_queue(f, args)

    `trajectories_key`: Dictionary key in input parameter dictionary holding the number of trajectories for target_function to perform. 
    
    `ensemble_key`: Dictionary key in input parameters containing the EnsembleAlgorithm for NQCD to use. 

    `tmp_dir`: Location for checkpoint files. 

    `checkpoint_frequency`: Number of operations per worker to perform before saving to a tempfile. (Default is 1 per worker - the faster and more stable, the higher to go)

    `sort_variable`: Offers the option to sort all jobs generated by a certain input parameter to group tasks of similar size together. 
TBW
"""
function pmap_queue(target_function, input_list::Vector{Dict}; trajectories_key="trajectories", ensemble_key="ensemble_algorithm", tmp_dir="tmp/", checkpoint_frequency=0, sort_variable="", afterburners=false)
    # Make temp dir, if it doesn't already exist
    if !isdir(tmp_dir)
        mkdir(tmp_dir)
    end
    # Give each combination of input parameters a unique ID to allow for easier merging later. 
    for i in 1:length(input_list)
        input_list[i]["jobid"]=i
    end
    # The simulation queue is organised by indices from the input_list to avoid loading many copies of the same input data. 
    simulation_queue=collect(1:length(input_list))
    # Threaded NQCD jobs are split into jobs per worker. 
    multithread_jobs=findall(x-> x[ensemble_key]==EnsembleThreads() && x[trajectories_key]>1, input_list)
    for index in multithread_jobs
        total_trj=input_list[index][trajectories_key]
        # Number of chunks should be number of workers or number of trajectories, if that is lower
        chunks=total_trj<length(workers()) ? total_trj : length(workers())
        # Chunk size should be 1 if less workers than trajectories, otherwise trajectories/workers rounded down to the nearest int. 
        chunk_size=total_trj<length(workers()) ? 1 : Int(floor(total_trj/length(workers())))
        # The total number of trajectories is preserved by splitting into >=2 chunks, whereby one of them takes on the remainder, if there is one. 
        modified_parameters=input_list[index]
        modified_parameters[trajectories_key]=chunk_size+total_trj%chunks
        input_list[index][trajectories_key]=chunk_size
        # Add to the simulation queue
        push!(input_list, modified_parameters)
        push!(simulation_queue, length(input_list))
        for n in 3:Int(chunks)
            push!(simulation_queue, index)
        end
    end
    # Serial and distributed NQCD jobs are split into single trajectory runs for checkpointing. 
    serial_jobs=findall(x-> (x[ensemble_key]==EnsembleSerial() || x[ensemble_key]==EnsembleDistributed()) && x[trajectories_key]>1, input_list)
    for index in serial_jobs
        n_traj=input_list[index][trajectories_key]
        input_list[index][trajectories_key]=1
        for n in 1:n_traj-1
            push!(simulation_queue, index)
        end
    end
    # All operations should now be atomised. Create a view to pmap target function over. 
    simulation_queue=input_list[simulation_queue]
    # Calculations are now split into chunks, which should take roughly the same amount of work to complete, otherwise one slow process will hold up all others, as all work must stop before a checkpoint. 
    # Put chunks of the same job in order. 
    sort!(simulation_queue, by=x->x["jobid"])
    # If a sort_variable is defined, the simulation queue is sorted by it (low to high)
    if sort_variable==""
        # If no sort order is specified, just work on the array in the order it's already in. 
        spx=Colon()
        spx_rev=Colon()
    else
        # Permutations for sorting forward
        spx=sortperm(simulation_queue, by=x->x[sort_variable])
        # Permutations to sort back into original order, so as not to destroy the variable order.  
        spx_rev=sortperm(spx)
    end
    # Now apply the sort order, or stay in the original order. 
    active_simulation_queue=view(simulation_queue, spx)
    if afterburners
        @info "Running a quick simulation to force precompile"
        # Run a short version of whatever job to force target_function to precompile
        short_params=copy(active_simulation_queue[1])
        short_params["runtime"]=short_params["timestep"]
        short_params["saveat"]=short_params["timestep"]
        pmap(target_function, [short_params for i in eachindex(workers())])
    end
    @info "Now starting simulation queue:"
    # Target function must provide output as Tuples (output from run_dynamics, input data)
    @time "Simulation queue" results=RobustPmap.crpmap(target_function, checkpoint_frequency==0 ? length(workers()) : length(workers())*checkpoint_frequency, tmp_dir*"tempfile", active_simulation_queue)
    # Since pmap creates a new array, sort it back into original order. 
    unsorted_results=results[spx_rev]
    # Process results, merging all split jobs back together. 
    return merge_pmap_results(unsorted_results;trajectories_key=trajectories_key)
end

"""
    merge_pmap_results(simulation_output;trajectories_key="trajectories", ensemble_key="ensemble_algorithm")
    Merger function for NQCD simulation results. Takes a vector of (NQCD output, input_parameters) tuples and merges together all unique combinations of simulation parameters, adding the number of trajectories together as if the simulation was run as a larger ensemble. 
TBW
"""
function merge_pmap_results(simulation_output::AbstractArray;trajectories_key="trajectories", ensemble_key="ensemble_algorithm")
    @info "Now merging simulation results"
    # Find all unique combinations of simulation parameters processed
    jobids=unique([simulation_output[i][2]["jobid"] for i in eachindex(simulation_output)])
    for id in jobids
        to_merge=findall(x->x[2]["jobid"]==id, simulation_output)
        if length(to_merge)>1
            simulation_output[to_merge[1]]=push_nqcd_outputs!(simulation_output[to_merge]...; trajectories_key=trajectories_key)
            # Get rid of the output of all merged results, in reverse to preserve indices. 
            for i in reverse(to_merge[2:end])
                popat!(simulation_output, i)
            end
        end
    end
    # Finally sort the simulation results in case something was misaligned
    sort!(simulation_output, by=x->x[2]["jobid"])
    return simulation_output
end

function merge_file_results(output_filename::String, glob_pattern::String, queue_file::String;trajectories_key="trajectories")
    # Make a struct for common file actions
    # Read in all files for a simulation queue.
    all_files=map(SimulationFile,glob(glob_pattern))
    progress=ProgressBar(total=length(all_files), printing_delay=1.0)
    set_description(progress, "Processing files: ")
    # Import simulation parameters
    simulation_parameters=jldopen(queue_file)
    # Create an empty total output object
    output_tensor=Array{Tuple}(undef, (size(simulation_parameters["parameters"])))
    # Go through each element in the input tensor and collect all jobs we have for it. 
    for index in eachindex(simulation_parameters["parameters"])
        to_read=findall(x->split(x.name, "_")[end] in map(string,simulation_parameters["parameters"][index]["job_ids"]), all_files)
        for file_index in to_read
            file_results=jldopen(all_files[file_index].path)["results"]
            if !isassigned(output_tensor, index)
                output_tensor[index]=file_results
            else
                output_tensor[index]=push_nqcd_outputs!(output_tensor[index], [file_results]; trajectories_key=trajectories_key)
            end
            update(progress)
        end
        # Trajectory completeness check
        if !isassigned(output_tensor, index) && output_tensor[index][2]["total_trajectories"]!=output_tensor[index][2]["trajectories"]
            @warn "Simulation results are incomplete or oversubscribed. Make sure you have run all sub-jobs. "
        end
    end
    jldsave(output_filename; results=output_tensor)
    return output_tensor
end

"""
    push_nqcd_outputs!!(first_output, other_outputs...)

    Like a push!() function, but it also puts `first_output` into a vector if it wasn't already and adds the number of trajectories together.  
TBW
"""
function push_nqcd_outputs!(first_output, other_outputs; trajectories_key="trajectories")
    for i in other_outputs
        for (k,v) in i[2]
            if k==trajectories_key
                first_output[2][trajectories_key]+=v
            end
        end
        if !isa(first_output[1], Vector)
            #? Can't modify first_output[1] directly due to data type of the results tuple. 
            first_output=([first_output[1]], first_output[2])
        end
        if isa(i[1], Vector)
            push!(first_output[1], i[1]...)
        else
            push!(first_output[1], i[1])
        end
    end
    return first_output
end

"""
    build_job_queue(fixed_parameters::Dict, variables::Dict)
    Returns a Vector of all unique combinations of values in `variables` merged with `fixed_parameters`. 
TBW
"""
function build_job_queue(fixed_parameters::Dict, variables::Dict)
    merged_combinations=Vector{Dict}()
    variable_combinations=reshape(collect(Iterators.product(values(variables)...)), :)
    for i in eachindex(variable_combinations)
        push!(merged_combinations, merge(fixed_parameters, Dict([(collect(keys(variables))[j], variable_combinations[i][j]) for j in 1:length(keys(variables))])))
    end
    return merged_combinations
end

"""
    build_job_queue(fixed_parameters::Dict, variables::Dict, postprocessing_function::Function)

    Returns a Vector of all unique combinations of values in `variables` merged with `fixed_parameters`. 
    By specifying a `postprocessing_function`, further actions can be performed each of the elements in the resulting vector. 
"""
function build_job_queue(fixed_parameters::Dict, variables::Dict, postprocessing_function::Function)
    merged_combinations=Vector{Dict}()
    variable_combinations=reshape(collect(Iterators.product(values(variables)...)), :)
    for i in eachindex(variable_combinations)
        push!(merged_combinations, merge(fixed_parameters, Dict([(collect(keys(variables))[j], variable_combinations[i][j]) for j in 1:length(keys(variables))])))
    end
    # Accept a function that does in-place modification of the input parameters dictionary
    return map(postprocessing_function,merged_combinations)
end

function serialise_queue!(input_dict_tensor; trajectories_key="trajectories", filename="simulation_parameters.jld2")
    queue=[] #Empty queue array to fill with views of input_dict_tensor
    job_id=1
    for index in eachindex(input_dict_tensor)
        # Save a list of jobs created from an input dict within it. 
        input_dict_tensor[index]["job_ids"]=[]
        # Save the total number of trajectories before modification of the input dict to verify completeness on analysis. 
        input_dict_tensor[index]["total_trajectories"]=input_dict_tensor[index][trajectories_key]
        if get!(input_dict_tensor[index],"batchsize",1)==1
            # Case 1: Fully serialised operation - Split into as many jobs as trajectories. 
            for trj in 1:input_dict_tensor[index][trajectories_key]
                # Add a view of the input dict 
                push!(queue, view(input_dict_tensor, index))
                push!(input_dict_tensor[index]["job_ids"], job_id)
                job_id+=1
            end
            input_dict_tensor[index][trajectories_key]=1
        else 
            # Case 2: Larger batch size - There might be some benefit like multithreading, so split into chunks of a certain size. 
            # If there enough trajectories to fit in >1 batch:
            for batch in 2:(floor(input_dict_tensor[index][trajectories_key]/input_dict_tensor[index]["batchsize"]))
                push!(queue, view(input_dict_tensor, index))
                push!(input_dict_tensor[index]["job_ids"], job_id)
                job_id+=1
            end
            extra_parameters=input_dict_tensor[index]
            extra_parameters[trajectories_key]+=input_dict_tensor[index][trajectories_key]%input_dict_tensor[index]["batchsize"]
            input_dict_tensor[index][trajectories_key]=floor(input_dict_tensor[index][trajectories_key]/input_dict_tensor[index]["batchsize"])
            push!(queue, hcat(extra_parameters)) # This covers any cases where the number of trajectories isn't exactly divisible by the batch size. 
            push!(input_dict_tensor[index]["job_ids"], job_id)
            job_id+=1
        end
    end
    jldsave(filename; parameters=input_dict_tensor, queue=queue)
end

export pmap_queue,build_job_queue, merge_pmap_results, merge_file_results, serialise_queue!, push_nqcd_outputs!

end
