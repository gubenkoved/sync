from setuptools import setup, find_namespace_packages


if __name__ == '__main__':
    setup(
        name='sync',
        version='0.0.1',
        packages=find_namespace_packages(where='src'),
        package_dir={'': 'src'},
        install_requires=[
        ],
    )
