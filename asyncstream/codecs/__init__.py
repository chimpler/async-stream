def error_import_usage(package: str):
    raise Exception('Package {package} missing. Please install it: pip install {package}'.format(package=package))
