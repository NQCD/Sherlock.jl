


function concatenate_results!(results_container::AbstractArray, glob_pattern::String, queue_file::String; trajectories_key="trajectories")
    # Read in all files for a simulation queue.
    glob_pattern = SimulationFile(glob_pattern)
    all_files = map(SimulationFile, glob(glob_pattern.with_extension, glob_pattern.stem))
    progress = ProgressBar(total=length(all_files), printing_delay=1.0)
    set_description(progress, "Processing files: ")
    # Import simulation parameters
    simulation_parameters = jldopen(queue_file)
    # Go through each element in the input tensor and collect all jobs we have for it.
    for index in eachindex(simulation_parameters["parameters"])
        # Read job ids from results if possible to avoid reading duplicates.
        job_ids = !isassigned(results_container, index) ? simulation_parameters["parameters"][index]["job_ids"] : container[index][2]["job_ids"]
        to_read = findall(x -> split(x.name, "_")[end] in string.(job_ids), all_files)
        for file_index in to_read
            try
                file_results = jldopen(all_files[file_index].path)["results"]
                @debug "File read successfully"
                # Move data to the output tensor
                if !isassigned(results_container, index)
                    results_container[index] = file_results
                else
                    results_container[index] = push_nqcd_outputs!(results_container[index], [file_results]; trajectories_key=trajectories_key)
                end
                # Remove job id from parameters once that result has been added
                jobid = parse(Int, split(all_files[file_index].name, "_")[end])
                deleteat!(results_container[index][2]["job_ids"], findall(results_container[index][2]["job_ids"] .== jobid)...)
            catch
                @warn "File $(all_files[file_index].name) could not be read. It may be incomplete or corrupted."
                continue
            end
            update(progress)
        end
        # Trajectory completeness check
        if !isassigned(results_container, index) || results_container[index][2]["total_trajectories"] != results_container[index][2]["trajectories"]
            @info "Simulation results are incomplete or oversubscribed in results[$(index)]. Make sure you have run all sub-jobs. "
        end
    end
end

function concatenate_results!(results_container::ResultsLazyLoader, glob_pattern::String, queue_file::String; trajectories_key="trajectories")
    # Read in all files for a simulation queue.
    glob_pattern = SimulationFile(glob_pattern)
    all_files = map(SimulationFile, glob(glob_pattern.with_extension, glob_pattern.stem))
    progress = ProgressBar(total=length(all_files), printing_delay=1.0)
    set_description(progress, "Processing files: ")
    # Import simulation parameters
    simulation_parameters = jldopen(queue_file)
    # Go through each element in the input tensor and collect all jobs we have for it.
    for index in eachindex(simulation_parameters["parameters"])
        # Read job ids from results if possible to avoid reading duplicates.
        job_ids = !haskey(results_container["results"], "$(index)") ? simulation_parameters["parameters"][index]["job_ids"] : container.parameters[index]["job_ids"]
        to_read = findall(x -> split(x.name, "_")[end] in string.(job_ids), all_files)
        for file_index in to_read
            try
                file_results = jldopen(all_files[file_index].path)["results"]
                @debug "File read successfully"
                # Move data to the output tensor
                if !haskey(results_container["results"], "$(index)")
                    results_container[index] = isa(file_results[1], Vector) ? file_results[1] : [file_results[1]]
                    results_container.parameters[index][trajectories_key] = file_results[2][trajectories_key]
                else
                    append!(results_container[index], file_results[1])
                    results_container.parameters[index][trajectories_key] += file_results[2][trajectories_key]
                end
                # Remove job id from parameters once that result has been added
                jobid = parse(Int, split(all_files[file_index].name, "_")[end])
                deleteat!(results_container.parameters[index]["job_ids"], findall(results_container.parameters[index]["job_ids"] .== jobid)...)
            catch e
                @warn "File $(all_files[file_index].name) could not be read. It may be incomplete or corrupted."
                @debug e
                continue
            end
            update(progress)
        end
        # Trajectory completeness check
        if !haskey(results_container["results"], "$(index)") || results_container.parameters[index]["total_trajectories"] != results_container.parameters[index]["trajectories"]
            @info "Simulation results are incomplete or oversubscribed in results[$(index)]. Make sure you have run all sub-jobs. "
        end
    end
end

"""
    push_nqcd_outputs!!(first_output, other_outputs...)

    Like a push!() function, but it also puts `first_output` into a vector if it wasn't already and adds the number of trajectories together.
TBW
"""
function push_nqcd_outputs!(first_output, other_outputs; trajectories_key="trajectories")
    for i in other_outputs
        for (k, v) in i[2]
            if k == trajectories_key
                first_output[2][trajectories_key] += v
            end
        end
        if !isa(first_output[1], Vector)
            #? Can't modify first_output[1] directly due to data type of the results tuple.
            first_output = ([first_output[1]], first_output[2])
        end
        if isa(i[1], Vector)
            push!(first_output[1], i[1]...)
        else
            push!(first_output[1], i[1])
        end
    end
    return first_output
end
