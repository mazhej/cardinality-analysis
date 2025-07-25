from sqlalchemy.orm import declarative_base

# Define the declarative base
Base = declarative_base()

# Import all models to register them with Base
# from ir_modernized.models.token import PersonToken, OrganizationToken
# from ir_modernized.models.delete import PersonDelete, ErrPersonDelete, OrganizationDelete, ErrOrganizationDelete

# import pkgutil
# import importlib
# # Dynamically import all modules in the `models` folder
# for _, module_name, _ in pkgutil.iter_modules(__path__):
#     importlib.import_module(f"{__name__}.{module_name}")