from setuptools import setup, find_packages

setup(
    name="adidas",
    version="0.1.0",
    license="test",
    author="ananth395",
    author_email="ananth395@gmail.com",
    description="Test for data engineer",
    zip_safe=False,
    include_package_data=True,
    packages=find_packages(
        include=["adidas", "adidas.*", "adidas.conf"], exclude=["adidas.tests"]
    ),
    test_suite="tests",
)
