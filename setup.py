from setuptools import setup
import os, io

from tslite import __version__

here = os.path.abspath(os.path.dirname(__file__))
README = io.open(os.path.join(here, 'README.md'), encoding='UTF-8').read()
CHANGES = io.open(os.path.join(here, 'CHANGES.md'), encoding='UTF-8').read()
setup(name="tslite",
      version=__version__,
      keywords=('TSDB', 'tslite', 'Time Series Database'),
      description="A Simple Time Series Database Implemented By Python.",
      long_description=README + '\n\n\n' + CHANGES,
      long_description_content_type="text/markdown",
      url='https://github.com/sintrb/tslite/',
      author="trb",
      author_email="sintrb@gmail.com",
      packages=['tslite'],
      zip_safe=False
      )
