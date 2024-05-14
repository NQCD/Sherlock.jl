module ClusterScripts

# Write your package code here.
using Distributed
using DiffEqBase
using JLD2
using RobustPmap
using Glob
using ProgressBars

"""
Struct to hold file paths and provide some basic functionality for working with them.
"""
struct SimulationFile
    path::String
    stem::String
    name::String
    with_extension::String
    function SimulationFile(full_path::String; path_delim="/")
        split_path = split(full_path, path_delim)
        stem = join(split_path[1:end-1], path_delim) * path_delim
        with_extension = split_path[end]
        name = join(split(with_extension, ".")[1:end-1], ".")
        new(full_path, stem, name, with_extension)
    end
end

struct ResultsLazyLoader
    file
    parameters::AbstractArray
    derived_quantities::AbstractArray
    function ResultsLazyLoader(file::String)
        open_file = jldopen(file, "r+")
        new(open_file, open_file["parameters"], open_file["derived_quantities"])
    end
end

include("concat_output.jl")
export concatenate_results!, push_nqcd_outputs!

include("pmap.jl")
export pmap_queue, merge_pmap_results

include("file_based.jl")
export build_job_queue, create_results_file, update_results_file, serialise_queue!

end
