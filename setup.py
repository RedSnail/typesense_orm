from setuptools import setup, find_packages


def parse_requirements(requirements):
    with open(requirements, "r") as req_file:
        return [line.strip('\n') for line in req_file if line.strip('\n')
                and not line.startswith('#')]


requires = parse_requirements("requirements.txt")

with open('README.md') as f:
    long_description = f.read()

setup(
    name="typesense_orm",
    packages=find_packages(),
    version="0.0.14",
    description="A typesense-server client using orm paradigm based on pydantic",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Oleg Demianchenko",
    author_email="oleg.demianchenko@gmail.com",
    license="MIT",
    platforms="OS Independent",
    url="https://github.com/RedSnail/typesense_orm",
    python_requires='>=3.8',
    include_package_data=False,
    build_requires=["wheel"],
    install_requires=requires
)
