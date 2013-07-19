from distutils.core import setup

setup(
      name='PyFlow',
      version='${VERSION}',
      description='Tool for running python workflows"',
      author='Chris Saunders',
      author_email='csaunders@illumina.com',
      packages=['pyflow'],
      package_dir={'pyflow': 'src'}
)
