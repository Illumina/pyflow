from distutils.core import setup

setup(
      name='pyFlow',
      version='${VERSION}',
      description='A lightweight parallel task engine',
      author='Chris Saunders',
      author_email='csaunders@illumina.com',
      packages=['pyflow'],
      package_dir={'pyflow': 'src'}
)
