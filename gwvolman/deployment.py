class Deployment(object):
    """Container for WT-specific docker stack deployment configuration.

    This class allows to read and store configuration of services in a WT
    deployment. It's meant to be used as a singleton across gwvolman.
    """
    
    def __init__(self):
        pass

    # should probably be called dashboard_external_url
    def dashboard_url(self):
        raise NotImplementedError()
        
    def girder_url(self):
        raise NotImplementedError()
    
    def registry_url(self):
        raise NotImplementedError()
    
    def docker_url(self):
        raise NotImplementedError()
