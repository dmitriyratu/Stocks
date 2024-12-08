import os
import site


def add_current_dir_to_site_packages():
    """
    Automatically add the current script's directory to Python's site-packages
    using a .pth file.
    """
    # Get the current script's directory (absolute path)
    current_dir = os.path.abspath(os.path.dirname(__file__))

    # Locate the user's site-packages directory
    site_packages_dir = site.getusersitepackages()

    # Ensure the site-packages directory exists
    os.makedirs(site_packages_dir, exist_ok=True)

    # Define the .pth file path
    pth_file = os.path.join(site_packages_dir, "current_project.pth")

    # Write the current directory path to the .pth file
    try:
        with open(pth_file, "w") as f:
            f.write(current_dir + "\n")
        print(f"Successfully added {current_dir} to {pth_file}")
    except Exception as e:
        print(f"Failed to create .pth file: {e}")


# Example usage
if __name__ == "__main__":
    add_current_dir_to_site_packages()
