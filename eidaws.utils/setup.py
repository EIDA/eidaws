import os

from setuptools import setup


def get_version(filename):
    from re import findall

    with open(filename) as f:
        metadata = dict(findall("__([a-z]+)__ = '([^']+)'", f.read()))
    return metadata["version"]


_AUTHOR = "Daniel Armbruster"
_AUTHOR_EMAIL = "daniel.armbruster@sed.ethz.ch"
_DESCRIPTION = "General purpose utilities for EIDA webservices"
_VERSION = get_version(os.path.join("eidaws", "utils", "__init__.py"))
_DEPS = [
    "ConfigArgParse>=1.2.3",
    "intervaltree>=3.0.2",
    "marshmallow==3.2.1",
    "python-dateutil>=2.6.1",
    "webargs==5.5.3",
    "PyYAML>=5.3",
]

setup(
    name="eidaws.utils",
    version=_VERSION,
    author=_AUTHOR,
    author_email=_AUTHOR_EMAIL,
    description=_DESCRIPTION,
    long_description=open("README.rst").read(),
    license="GPLv3",
    keywords="seismology waveforms eida service",
    url="https://github.com/damb/eidaws/eidaws.utils",
    platforms=["Linux"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
        "Topic :: Scientific/Engineering",
    ],
    packages=["eidaws.utils"],
    zip_safe=False,
    install_requires=_DEPS,
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    python_requires="~=3.6",
)
