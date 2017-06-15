import random
from .tableau_exceptions import *
from .logger import Logger
import re
from lxml import etree
from io import StringIO, BytesIO


class TableauBase(object):
    def __init__(self):
        # In reverse order to work down until the acceptable version is found on the server, through login process
        self.supported_versions = ("10.0", "9.3", "9.2", "9.1", "9.0")
        self.logger = None
        self.luid_pattern = r"[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*"

        # Defaults, will get updated with each update. Overwritten by set_tableau_server_version
        self.version = "10.0"
        self.api_version = "2.3"
        self.tableau_namespace = 'http://tableau.com/api'
        self.ns_map = {'t': 'http://tableau.com/api'}
        self.ns_prefix = '{' + self.ns_map['t'] + '}'

        self.site_roles = (
            'Interactor',
            'Publisher',
            'SiteAdministrator',
            'Unlicensed',
            'UnlicensedWithPublish',
            'Viewer',
            'ViewerWithPublish',
            'ServerAdministrator'
        )

        server_content_roles_2_0 = {
                "project": (
                    'Viewer',
                    'Interactor',
                    'Editor',
                    'Data Source Connector',
                    'Data Source Editor',
                    'Publisher',
                    'Project Leader'
                ),
                "workbook": (
                    'Viewer',
                    'Interactor',
                    'Editor'
                ),
                "datasource": (
                    'Data Source Connector',
                    'Data Source Editor'
                )
            }

        server_content_roles_2_1 = {
                "project": (
                    'Viewer',
                    'Publisher',
                    'Project Leader'
                ),
                "workbook": (
                    'Viewer',
                    'Interactor',
                    'Editor'
                ),
                "datasource": (
                    'Editor',
                    'Connector'
                )
            }

        self.server_content_roles = {
            "2.0": server_content_roles_2_0,
            "2.1": server_content_roles_2_1,
            "2.2": server_content_roles_2_1,
            "2.3": server_content_roles_2_1
        }

        self.server_to_rest_capability_map = {
            'Add Comment': 'AddComment',
            'Move': 'ChangeHierarchy',
            'Set Permissions': 'ChangePermissions',
            'Connect': 'Connect',
            'Delete': 'Delete',
            'View Summary Data': 'ExportData',
            'Export Image': 'ExportImage',
            'Download': 'ExportXml',
            'Filter': 'Filter',
            'Project Leader': 'ProjectLeader',
            'View': 'Read',
            'Share Customized': 'ShareView',
            'View Comments': 'ViewComments',
            'View Underlying Data': 'ViewUnderlyingData',
            'Web Edit': 'WebAuthoring',
            'Save': 'Write',
            'all': 'all'  # special command to do everything
        }

        capabilities_2_0 = {
                "project": (
                    'AddComment',
                    'ChangeHierarchy',
                    'ChangePermissions',
                    'Connect',
                    'Delete',
                    'ExportData',
                    'ExportImage',
                    'ExportXml',
                    'Filter',
                    'ProjectLeader',
                    'Read',
                    'ShareView',
                    'ViewComments',
                    'ViewUnderlyingData',
                    'WebAuthoring',
                    'Write'
                ),
                "workbook": (
                    'AddComment',
                    'ChangeHierarchy',
                    'ChangePermissions',
                    'Delete',
                    'ExportData',
                    'ExportImage',
                    'ExportXml',
                    'Filter',
                    'Read',
                    'ShareView',
                    'ViewComments',
                    'ViewUnderlyingData',
                    'WebAuthoring',
                    'Write'
                ),
                "datasource": (
                    'ChangePermissions',
                    'Connect',
                    'Delete',
                    'ExportXml',
                    'Read',
                    'Write'
                )
            }

        capabilities_2_1 = {
                "project": ("Read", "Write", 'ProjectLeader'),
                "workbook": (
                    'Read',
                    'ExportImage',
                    'ExportData',
                    'ViewComments',
                    'AddComment',
                    'Filter',
                    'ViewUnderlyingData',
                    'ShareView',
                    'WebAuthoring',
                    'Write',
                    'ExportXml',
                    'ChangeHierarchy',
                    'Delete',
                    'ChangePermissions',

                ),
                "datasource": (
                    'Read',
                    'Connect',
                    'Write',
                    'ExportXml',
                    'Delete',
                    'ChangePermissions'
                )
            }

        self.available_capabilities = {
            "2.0": capabilities_2_0,
            "2.1": capabilities_2_1,
            "2.2": capabilities_2_1,
            '2.3': capabilities_2_1
        }

        self.datasource_class_map = {
            "Actian Vectorwise": "vectorwise",
            "Amazon EMR": "awshadoophive",
            "Amazon Redshift": "redshift",
            "Aster Database": "asterncluster",
            "Cloudera Hadoop": "hadoophive",
            "DataStax Enterprise": "datastax",
            "EXASolution": "exasolution",
            "Firebird": "firebird",
            "Generic ODBC": "genericodbc",
            "Google Analytics": "google-analytics",
            "Google BigQuery": "bigquery",
            "Hortonworks Hadooop Hive": "hortonworkshadoophive",
            "HP Vertica": "vertica",
            "IBM BigInsights": "bigsql",
            "IBM DB2": "db2",
            "JavaScript Connector": "jsconnector",
            "MapR Hadoop Hive": "maprhadoophive",
            "MarkLogic": "marklogic",
            "Microsoft Access": "msaccess",
            "Microsoft Analysis Services": "msolap",
            "Microsoft Excel": "",
            "Microsoft PowerPivot": "powerpivot",
            "Microsoft SQL Server": "sqlserver",
            "MySQL": "mysql",
            "IBM Netezza": "netezza",
            "OData": "odata",
            "Oracle": "oracle",
            "Oracle Essbase": "essbase",
            "ParAccel": "paraccel",
            "Pivotal Greenplum": "greenplum",
            "PostgreSQL": "postgres",
            "Progress OpenEdge": "progressopenedge",
            "SAP HANA": "saphana",
            "SAP Netweaver Business Warehouse": "sapbw",
            "SAP Sybase ASE": "sybasease",
            "SAP Sybase IQ": "sybaseiq",
            "Salesforce": "salesforce",
            "Spark SQL": "spark",
            "Splunk": "splunk",
            "Statistical File": "",
            "Tableau Data Extract": "dataengine",
            "Teradata": "teradata",
            "Text file": "csv"
        }

        self.permissionable_objects = ('datasource', 'project', 'workbook')

    def set_tableau_server_version(self, tableau_server_version):
        """
        :type tableau_server_version: unicode
        """
        # API Versioning (starting in 9.2)
        if str(tableau_server_version)in ["9.2", "9.3", "10.0"]:
            if str(tableau_server_version) == "9.2":
                self.api_version = "2.1"
            elif str(tableau_server_version) == "9.3":
                self.api_version = "2.2"
            elif str(tableau_server_version) == '10.0':
                self.api_version = '2.3'
            self.tableau_namespace = 'http://tableau.com/api'
            self.ns_map = {'t': 'http://tableau.com/api'}
            self.version = tableau_server_version
            self.ns_prefix = '{' + self.ns_map['t'] + '}'
        elif str(tableau_server_version) in ["9.0", "9.1"]:
            self.api_version = "2.0"
            self.tableau_namespace = 'http://tableausoftware.com/api'
            self.ns_map = {'t': 'http://tableausoftware.com/api'}
            self.version = tableau_server_version
            self.ns_prefix = '{' + self.ns_map['t'] + '}'
        else:
            raise InvalidOptionException("Please specify tableau_server_version as a string. '9.0' or '9.2' etc...")

    # Logging Methods
    def enable_logging(self, logger_obj):
        if isinstance(logger_obj, Logger):
            self.logger = logger_obj

    def log(self, l):
        if self.logger is not None:
            self.logger.log(l)

    def start_log_block(self):
        if self.logger is not None:
            self.logger.start_log_block()

    def end_log_block(self):
        if self.logger is not None:
            self.logger.end_log_block()

    def log_uri(self, uri, verb):
        if self.logger is not None:
            self.logger.log_uri(verb, uri)

    def log_xml_request(self, xml, verb):
        if self.logger is not None:
            self.logger.log_xml_request(verb, xml)

    # Method to handle single str or list and return a list
    @staticmethod
    def to_list(x):
        if isinstance(x, str):
            l = [x]  # Make single into a collection
        else:
            l = x
        return l

    # Method to read file in x MB chunks for upload, 10 MB by default (1024 bytes = KB, * 1024 = MB, * 10)
    @staticmethod
    def read_file_in_chunks(file_object, chunk_size=(1024 * 1024 * 10)):
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    # You must generate a boundary string that is used both in the headers and the generated request that you post.
    # This builds a simple 30 hex digit string
    @staticmethod
    def generate_boundary_string():
        random_digits = [random.SystemRandom().choice('0123456789abcdef') for n in range(30)]
        s = "".join(random_digits)
        return s

    # URI is different form actual URL you need to load a particular view in iframe
    @staticmethod
    def convert_view_content_url_to_embed_url(content_url):
        split_url = content_url.split('/')
        return 'views/' + split_url[0] + "/" + split_url[2]

    # Generic method for XML lists for the "query" actions to name -> id dict
    @staticmethod
    def convert_xml_list_to_name_id_dict(lxml_obj):
        d = {}
        for element in lxml_obj:
            e_id = element.get("id")
            # If list is collection, have to run one deeper
            if e_id is None:
                for list_element in element:
                    e_id = list_element.get("id")
                    name = list_element.get("name")
                    d[name] = e_id
            else:
                name = element.get("name")
                d[name] = e_id
        return d

    # Convert a permission
    def convert_server_permission_name_to_rest_permission(self, permission_name):
        if permission_name in self.server_to_rest_capability_map:
            return self.server_to_rest_capability_map[permission_name]
        else:
            raise InvalidOptionException('{} is not a permission name on the Tableau Server'.format(permission_name))

    # 32 hex characters with 4 dashes
    def is_luid(self, val):
        if len(val) == 36:
            if re.match(self.luid_pattern, val) is not None:
                return True
            else:
                return False
        else:
            return False

    # Looks at LUIDs in new_obj_list, if they exist in the dest_obj, compares their gcap objects, if match returns True
    def are_capabilities_objs_identical_for_matching_luids(self, new_obj_list, dest_obj_list):
        self.start_log_block()
        # Create a dict with the LUID as the keys for sorting and comparison
        new_obj_dict = {}
        for obj in new_obj_list:
            new_obj_dict[obj.get_luid()] = obj

        dest_obj_dict = {}
        for obj in dest_obj_list:
            dest_obj_dict[obj.get_luid()] = obj

        new_obj_luids = list(new_obj_dict.keys())
        dest_obj_luids = list(dest_obj_dict.keys())

        if set(dest_obj_luids).issuperset(new_obj_luids):
            # At this point, we know the new_objs do exist on the current obj, so let's see if they are identical
            for luid in new_obj_luids:
                new_obj = new_obj_dict.get(luid)
                dest_obj = dest_obj_dict.get(luid)

                self.log("Capabilities to be set:")
                new_obj_cap_dict = new_obj.get_capabilities_dict()
                self.log(str(new_obj_cap_dict))
                self.log("Capabilities that were originally set:")
                dest_obj_cap_dict = dest_obj.get_capabilities_dict()
                self.log(str(dest_obj_cap_dict))
                if new_obj_cap_dict == dest_obj_cap_dict:
                    self.end_log_block()
                    return True
                else:
                    self.end_log_block()
                    return False
        else:
            self.end_log_block()
            return False

    # Determine if capabilities are already set identically (or identically enough) to skip
    def are_capabilities_obj_lists_identical(self, new_obj_list, dest_obj_list):
        # Grab the LUIDs of each, determine if they match in the first place

        # Create a dict with the LUID as the keys for sorting and comparison
        new_obj_dict = {}
        for obj in new_obj_list:
            new_obj_dict[obj.get_luid()] = obj

        dest_obj_dict = {}
        for obj in dest_obj_list:
            dest_obj_dict[obj.get_luid()] = obj
            # If lengths don't match, they must differ
            if len(new_obj_dict) != len(dest_obj_dict):
                return False
            else:
                # If LUIDs don't match, they must differ
                new_obj_luids = list(new_obj_dict.keys())
                dest_obj_luids = list(dest_obj_dict.keys())
                new_obj_luids.sort()
                dest_obj_luids.sort()
                if cmp(new_obj_luids, dest_obj_luids) != 0:
                    return False
                for luid in new_obj_luids:
                    new_obj = new_obj_dict.get(luid)
                    dest_obj = dest_obj_dict.get(luid)
                    return self.are_capabilities_obj_dicts_identical(new_obj.get_capabilities_dict(),
                                                                     dest_obj.get_capabilities_dict())

    @staticmethod
    def are_capabilities_obj_dicts_identical(new_obj_dict, dest_obj_dict):
        if cmp(new_obj_dict, dest_obj_dict) == 0:
            return True
        else:
            return False

    # Dict { capability_name : mode } into XML with checks for validity. Set type to 'workbook' or 'datasource'
    def build_capabilities_xml_from_dict(self, capabilities_dict, obj_type):
        if obj_type not in self.permissionable_objects:
            error_text = 'objtype can only be "project", "workbook" or "datasource", was given {}'
            raise InvalidOptionException(error_text.format('obj_type'))
        xml = '<capabilities>\n'
        for cap in capabilities_dict:
            # Skip if the capability is set to None
            if capabilities_dict[cap] is None:
                continue
            if capabilities_dict[cap] not in ['Allow', 'Deny']:
                raise InvalidOptionException('Capability mode can only be "Allow",  "Deny" (case-sensitive)')
            if obj_type == 'project':
                if cap not in self.available_capabilities[self.api_version]["project"]:
                    raise InvalidOptionException('{} is not a valid capability for a project'.format(cap))
            if obj_type == 'datasource':
                # Ignore if not available for datasource
                if cap not in self.available_capabilities[self.api_version]["datasource"]:
                    self.log('{} is not a valid capability for a datasource'.format(cap))
                    continue
            if obj_type == 'workbook':
                # Ignore if not available for workbook
                if cap not in self.available_capabilities[self.api_version]["workbook"]:
                    self.log('{} is not a valid capability for a workbook'.format(cap))
                    continue
            xml += '<capability name="{}" mode="{}" />'.format(cap, capabilities_dict[cap])
        xml += '</capabilities>'
        return xml

