from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install

install_requires = [
    'XlsxWriter',
    'pandas',
    'requests',
    'lxml',
    'gherkin-official<=15',
    'bdd-tester==0.0.6',
]

setup(
    name="iati-colab",
    version="0.1",
    author="Open Data Services",
    author_email="code@opendataservices.coop",
    py_modules=["iati_colab"],
    url="https://github.com/OpenDataServices/iati-colab",
    license="MIT",
    description="Tools of doing iati analysis in google colab",
    install_requires=install_requires,
)
