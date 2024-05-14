using Documenter
using ClusterScripts

makedocs(
    sitename = "ClusterScripts",
    format = Documenter.HTML(),
    modules = [ClusterScripts]
)

# Documenter can also automatically deploy documentation to gh-pages.
# See "Hosting Documentation" and deploydocs() in the Documenter manual
# for more information.
#=deploydocs(
    repo = "<repository url>"
)=#
