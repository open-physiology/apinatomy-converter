from distutils.core import setup

setup(
    name='apinatomy',
    version='1',
    packages=[''],
    url='',
    license='',
    author='Natallia Kokash',
    author_email='natallia.kokash@gmail.com',
    description='Generates cardio-vascular graph from MySQL DB',
    install_requires=['mysql', 'neo4j', 'numpy', 'pandas', 'gspread', 'json']
)
