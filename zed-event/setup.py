import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='zed-event',
     version='0.1',
     scripts=['zed_event.py'] ,
     author="California Digital Library",
     author_email="charlie.collett@ucop.edu",
     description="A package for creating and validating Zed events.",
     long_description=long_description,
     long_description_content_type="text/markdown",
     url="https://github.com/cdlib/zephir-services",
     zip_safe=False,
     packages=setuptools.find_packages(),
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
     install_requires=['jsonschema'],
 )
