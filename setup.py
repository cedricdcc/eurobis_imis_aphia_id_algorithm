"""
Setup configuration for the eurobis_imis_aphia_id_algorithm package.
"""

from setuptools import setup, find_packages

# Read README file
try:
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()
except FileNotFoundError:
    long_description = "Algorithm to determine which Aphia IDs to use"

setup(
    name="eurobis-imis-aphia-id-algorithm",
    version="1.0.0",
    author="Cedric DCC",
    author_email="cedric@example.com",
    description="Algorithm to determine which Aphia IDs to use",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cedricdcc/eurobis_imis_aphia_id_algorithm",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
    install_requires=[
        "pandas>=1.0.0",
        "numpy>=1.18.0",
        "requests>=2.20.0",
        "plotly>=4.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.0.0",
            "black>=21.0.0",
            "isort>=5.0.0",
            "flake8>=3.8.0",
        ],
        "database": [
            "pyodbc>=4.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "aphia-algorithm=eurobis_imis_aphia_id_algorithm.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)