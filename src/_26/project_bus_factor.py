from os import system, path


def gavelino_truck_factor(base_path: str = "", git_repository_path: str = "", git_repository_fullname: str = "", linguist: bool = False):
    """
    Executes the Gavelino Truck Factor analysis on a specified Git repository.

    This function automates the process of calculating the Truck Factor, which 
    measures the number of key contributors that can leave a project before it 
    becomes unsustainable. It achieves this by running a series of scripts and 
    a Java program provided by the Gavelino Truck Factor tool.

    Args:
        base_path (str, optional): The base directory containing the `gittruckfactor` 
            folder with the required scripts and Java program. Defaults to an empty string.
        git_repository_path (str, optional): The file path to the Git repository 
            to be analyzed. Defaults to an empty string.
        git_repository_fullname (str, optional): The full name of the Git repository 
            (e.g., "username/repository"). Defaults to an empty string.
        linguist (bool, optional): If True, applies a linguistic filter using the 
            `linguist_script.sh` script before running the analysis. Defaults to False.

    Raises:
        OSError: If any of the system commands fail to execute properly.

    Notes:
        - The `commit_log_script.sh` script is always executed to process the commit history.
        - The `linguist_script.sh` script is executed only if the `linguist` parameter is True.
        - The Java program `GitTruckFactor.java` performs the final Truck Factor analysis.

    Example:
        gavelino_truck_factor(
            base_path="/path/to/gavelino",
            git_repository_path="/path/to/repository",
            git_repository_fullname="username/repository",
            linguist=True
        )
    """

    #base_path -> git clone of gavelino's Truck-Factor
    #git_repository_path -> git clone target
    #linguist: bool -> Liguistic's filter
    system(f"{path.join(base_path, "gittruckfactor", "scripts", "commit_log_script.sh")} {git_repository_path}")
    # scripts -> ./commit_log_script.sh <git_repository_path>

    if(linguist):
        system(f"{path.join(base_path, "gittruckfactor", "scripts", "linguist_script.sh")} {git_repository_path}")
        # optional -> ./linguist_script.sh <git_repository_path>

    system(f"java -jar {path.join(base_path, "gittruckfactor", "src", "aserg", "gtf", "GitTruckFactor.java")} {git_repository_path} {git_repository_fullname}")
    # java -jar gittruckfactor.jar <git_repository_path> <git_repository_fullname>

def main():
    ...

if __name__ == "__main__":
    main()