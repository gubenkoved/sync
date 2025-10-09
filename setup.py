from setuptools import find_namespace_packages, setup

if __name__ == "__main__":
    setup(
        name="sync",
        version="0.6.1",
        packages=find_namespace_packages(where="src"),
        package_dir={"": "src"},
        install_requires=[
            "coloredlogs",
            "dropbox",
            "paramiko",
        ],
        entry_points={
            "console_scripts": ["egsync=sync.cli:entrypoint"],
        },
    )
