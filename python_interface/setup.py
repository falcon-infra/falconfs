from setuptools import setup, find_packages

setup(
    name="pyfalconfs",
    version="0.1",
    packages=find_packages(),
    package_data={
        "pyfalconfs": ["_pyfalconfs_internal.so", "throw_hook.so"]
    },
    include_package_data=True
)