from distutils.core import setup

setup(
    name="grade_process_mod",
    packages=["grade_process_mod"],
    version="0.1",
    license="MIT",
    description="Process Freeval grade data",
    author="Apoorb",
    author_email="apoorb2510@gmail.com",
    url="https://github.com/Apoorb/Freeval-Data-Processing",
    download_url="",  # Fill later
    keywords=["GRADE DATA", "FREEVAL", "HCM PCE"],
    install_requires=["numpy", "pandas",],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: KAI Freeval Team",
        "Topic :: Data Processing",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
    ],
)
