from setuptools import setup, find_packages
with open("requirements.txt") as f:
    requirements = f.read().splitlines()
setup(
    name="Broker",
    version="1.1.0",
    author="SadalSuud",
    author_email="kathancpandya@gmail.com",
    description="Indian Stock Market Broker's Library",
    packages=find_packages(),
    install_requires=requirements,
    python_requires=">=3.9",
)

