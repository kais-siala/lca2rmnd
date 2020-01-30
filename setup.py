from setuptools import setup

setup(
    name='lca2rmnd',
    url='https://github.com/Loisel/lca2rmnd',
    author='Alois Dirnaichner',
    author_email='alodi@directbox.com',
    packages=['lca2rmnd'],
    install_requires=['numpy', 'brightway2'],
    version='0.1',
    license='MIT',
    description='Report LCA impacts for sectors and technologies based on REMIND output.',
)
