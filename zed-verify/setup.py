from setuptools import setup

setup(
    name="verify",
    version="0.1",
    description="Verify ZED-style log files.",
    url="http://github.com/cdlib/zephir-services",
    author="CDL",
    author_email="zephir-help@ucop.edu",
    license="BSD",
    scripts=["zephir-verify"],
    install_requires=["environs", "sqlalchemy", "pyyaml"],
    zip_safe=False,
)
