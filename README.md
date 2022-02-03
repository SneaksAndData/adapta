# Template repository for Python projects
Use this repo to create new python projects with ready-to-go build and deploy pipelines, folder structure and `setup.py` configured for git versioning.

## Checklist

Remember to do the following after creating a new repo based on this one:

- :heavy_check_mark: Search and replace `<python_project>` with your desired package name (repository name)
- :heavy_check_mark: Update **at least** package name and `install_requires` in `setup.py`
- :heavy_check_mark: Select build and deploy pipelines to use in `workflows/` dir. Usually you will need `build.yaml` and `deploy-azure-artifacts.yaml`. 
  - Remove dummy code and uncomment real code
  - Remove pipelines you won't need

Happy coding!
