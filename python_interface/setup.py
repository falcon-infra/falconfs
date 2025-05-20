from setuptools import setup, find_packages

setup(
    name="pyfalconfs",
    version="1.0.0",
    packages=find_packages(),
    package_data={
        "pyfalconfs": ["_pyfalconfs_internal.so", "throw_hook.so"]
    },
    include_package_data=True
)