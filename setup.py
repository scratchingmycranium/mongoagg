from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()
    
setup(
    name="mongoagg",
    version="0.1.3",
    description="MongoDB Aggregation Pipeline Builder",
    author="scratchingmycranium",
    author_email="41268767+scratchingmycranium@users.noreply.github.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/scratchingmycranium/mongoagg",
    packages=find_packages(),
    install_requires=[
        "pydantic",
        "bson",
        "pytest",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
) 