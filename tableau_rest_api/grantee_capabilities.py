from ..tableau_base import TableauBase
from ..tableau_exceptions import *


# Represents the GranteeCapabilities from any given.
class GranteeCapabilities(TableauBase):
    def __init__(self, obj_type, luid, content_type=None, tableau_server_version="9.2"):
        super(self.__class__, self).__init__()
        self.set_tableau_server_version(tableau_server_version)
        if obj_type not in ['group', 'user']:
            raise InvalidOptionException('GranteeCapabilites type must be "group" or "user"')
        self.content_type = content_type
        self.obj_type = obj_type
        self.luid = luid
        # Get total set of capabilities, set to None by default
        self.__capabilities = {}
        self.__server_to_rest_capability_map = self.server_to_rest_capability_map
        self.__allowable_modes = ['Allow', 'Deny', None]
        if content_type is not None:
            # Defined in TableauBase superclass
            self.__role_map = self.server_content_roles[self.api_version][content_type]
            for cap in self.available_capabilities[self.api_version][content_type]:
                if cap != 'all':
                    self.__capabilities[cap] = None

    def set_capability(self, capability_name, mode):
        if capability_name not in list(self.__server_to_rest_capability_map.values()):
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.__server_to_rest_capability_map:
                if capability_name != 'all':
                    capability_name = self.__server_to_rest_capability_map[capability_name]
            else:
                raise InvalidOptionException('"{}" is not a capability in REST API or Server'.format(capability_name))
        self.__capabilities[capability_name] = mode

    def set_capability_to_unspecified(self, capability_name):
        if capability_name not in self.__capabilities:
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.__server_to_rest_capability_map:
                if capability_name != 'all':
                    capability_name = self.__server_to_rest_capability_map[capability_name]
            else:
                raise InvalidOptionException('"{}" is not a capability in REST API or Server'.format(capability_name))
        self.__capabilities[capability_name] = None

    def get_capabilities_dict(self):
        return self.__capabilities

    def get_obj_type(self):
        return self.obj_type

    def get_luid(self):
        return self.luid

    def set_obj_type(self, obj_type):
        if obj_type.lower() in ['group', 'user']:
            self.obj_type = obj_type.lower()
        else:
            raise InvalidOptionException('obj_type can only be "group" or "user"')

    def set_luid(self, new_luid):
        self.luid = new_luid

    def set_all_to_deny(self):
        for cap in self.__capabilities:
            if cap != 'all':
                self.__capabilities[cap] = 'Deny'

    def set_all_to_allow(self):
        for cap in self.__capabilities:
            if cap != 'all':
                self.__capabilities[cap] = 'Allow'

    def set_all_to_unspecified(self):
        for cap in self.__capabilities:
            if cap != 'all':
                self.__capabilities[cap] = None

    def set_capabilities_to_match_role(self, role):
        if role not in self.__role_map:
            raise InvalidOptionException('{} is not a recognized role'.format(role))

        # Clear any previously set capabilities
        self.__capabilities = {}

        role_set_91_and_earlier_all_types = {
            'Publisher': {
                'all': True,
                'Connect': None,
                'Download': None,
                'Move': None,
                'Delete': None,
                'Set Permissions': None,
                'Project Leader': None,
             },
            'Interactor': {
                'all': True,
                'Connect': None,
                'Download': None,
                'Move': None,
                'Delete': None,
                'Set Permissions': None,
                'Project Leader': None,
                'Save': None
            },
            'Viewer': {
                'View': 'Allow',
                'Export Image': 'Allow',
                'View Summary Data': 'Allow',
                'View Comments': 'Allow',
                'Add Comment': 'Allow'
            },
            'Editor': {
                'all': True,
                'Connect': None,
                'Project Leader': None
            },
            'Data Source Connector': {
                'all': None,
                'Connect': None,
                'Project Leader': None
            },
            'Data Source Editor': {
                'all': None,
                'View': 'Allow',
                'Connect': 'Allow',
                'Save': 'Allow',
                'Download': 'Allow',
                'Delete': 'Allow',
                'Set Permissions': 'Allow'
            },
            'Project Leader': {
                'all': None,
                'Project Leader': 'Allow'
            }
        }

        role_set_92 = {
            "project": {
                "Viewer": {
                    'all': None,
                    "View": "Allow"
                },
                "Publisher": {
                    'all': None,
                    "View": "Allow",
                    "Save": "Allow"
                },
                "Project Leader": {
                    'all': None,
                    "Project Leader": "Allow"
                }
            },
            "workbook": {
                "Viewer": {
                    'all': None,
                    'View': 'Allow',
                    'Export Image': 'Allow',
                    'View Summary Data': 'Allow',
                    'View Comments': 'Allow',
                    'Add Comment': 'Allow'
                },
                "Interactor": {
                    'all': True,
                    'Download': None,
                    'Move': None,
                    'Delete': None,
                    'Set Permissions': None,
                    'Save': None
                },
                "Editor": {
                    'all': True
                }
            },
            "datasource": {
                "Connector": {
                    'all': None,
                    'View': 'Allow',
                    'Connect': 'Allow'
                },
                "Editor": {
                    'all': True
                }
            }
        }

        role_set = {
            '2.0': {
                "project": role_set_91_and_earlier_all_types,
                "workbook": role_set_91_and_earlier_all_types,
                "datasource": role_set_91_and_earlier_all_types
            },
            '2.1': role_set_92,
            '2.2': role_set_92,
            '2.3': role_set_92
        }
        if role not in role_set[self.api_version][self.content_type]:
            raise InvalidOptionException("There is no role in Tableau Server available for {} called {}".format(
                self.content_type, role
            ))
        role_capabilities = role_set[self.api_version][self.content_type][role]
        if "all" in role_capabilities:
            if role_capabilities["all"] is True:
                self.set_all_to_allow()
            elif role_capabilities["all"] is False:
                self.set_all_to_deny()
        for cap in role_capabilities:
            # Skip the all command, we handled it at the beginning
            if cap == 'all':
                continue
            elif role_capabilities[cap] is not None:
                self.set_capability(cap, role_capabilities[cap])
            elif role_capabilities[cap] is None:
                self.set_capability_to_unspecified(cap)
