from setuptools import setup, find_packages

setup(
    name = 'mysqlfs',
    version = '1.0',
    packages = find_packages(),
    scripts = ['cmd/mysqlfs'],

    install_requires = ['six>=1.5.0', 'SQLAlchemy<1.1.0,>=0.5.5'],
    
    author = "Lan",
    author_email = "lqs933@foxmail.com",
    description = "This is Fuse file system",
)
