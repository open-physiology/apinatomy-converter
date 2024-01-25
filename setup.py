from distutils.core import setup

setup(
    name='sync-WBKG',
    version='1',
    packages=[''],
    scripts=['mysql-to-xlsx-wbkg'],
    data_files=[('', ['data/wbkg_db.json', 'data/service_account.json'])],
    entry_points={
        'console_scripts': [
            'sync=mysql-to-xlsx-wbkg'
        ],
    },
    url='',
    license='',
    author='Natallia Kokash',
    author_email='natallia.kokash@gmail.com',
    description='Synchronize WBKG between Google spreadsheet and MySQL DB',
    install_requires=['mysql', 'pandas', 'gspread', 'openpyx'gspread_dataframe']
)
