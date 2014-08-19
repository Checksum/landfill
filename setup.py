import landfill
from setuptools import setup

dependencies = ['peewee', 'termcolor']

setup(
    name='landfill',
    version=landfill.__version__,
    author='Srinath',
    author_email='srinath@iambot.net',
    description='Manage migrations for Peewee, Django style.',
    long_description='',
    keywords='python, peewee, migrations, orm',
    url='https://github.com/Checksum/landfill',
    license='MIT',
    py_modules=['landfill'],
    install_requires=dependencies,
    classifiers=[
        'Development Status :: 3 - Alpha',
        "Programming Language :: Python",
        'Topic :: Utilities',
        "Topic :: Software Development :: Libraries :: Python Modules",
        'License :: OSI Approved :: MIT License',
    ]
)
