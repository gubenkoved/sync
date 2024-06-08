from setuptools import setup, find_namespace_packages


if __name__ == '__main__':
    with open('requirements.txt') as f:
        requirements = [
            line for line in f.readlines()
            if not line.strip().startswith('#')
        ]

    setup(
        name='sync',
        version='0.3.0',
        packages=find_namespace_packages(where='src'),
        package_dir={'': 'src'},
        install_requires=requirements,
        entry_points={
            'console_scripts': ['egsync=sync.cli:entrypoint'],
        }
    )
