import datetime
from xml.sax.saxutils import quoteattr

from ..tableau_base import *
from ..tableau_exceptions import *

import zipfile
import os


class TableauDatasourceGenerator(TableauBase):
    def __init__(self, ds_type, ds_name, server, dbname, logger_obj, authentication='username-password',
                 initial_sql=None):
        super(self.__class__, self).__init__()
        self.logger = logger_obj
        self.log('Initializing a TableauDatasourceGenerator object')
        self.ds_class = None
        self.ds_name = ds_name
        if ds_type in self.datasource_class_map:
            self.ds_class = self.datasource_class_map[ds_type]
        elif ds_type in list(self.datasource_class_map.values()):
            self.ds_class = ds_type
        else:
            raise InvalidOptionException('{} is not an acceptable type'.format(ds_type))
        self.log("DS Class is {}".format(self.ds_class))
        self.nsmap = {"user": 'http://www.tableausoftware.com/xml/user'}
        self.ds_xml = etree.Element("datasource", nsmap=self.nsmap)
        self.ds_xml.set('formatted-name', self.ds_class + '.1ch1jwefjwfw')
        self.ds_xml.set('inline', 'true')
        self.ds_xml.set('version', '9.3')
        self.server = server
        self.dbname = dbname
        self.authentication = authentication
        self.main_table_relation = None
        self.main_table_name = None
        self.join_relations = []
        self.connection = etree.Element("connection")
        self.connection.set('class', self.ds_class)
        self.connection.set('dbname', self.dbname)
        self.connection.set('odbc-native-protocol', 'yes')
        if self.server is not None:
            self.connection.set('server', self.server)
        self.connection.set('authentication', 'sspi')
        if initial_sql is not None:
            self.connection.set('one-time-sql', initial_sql)
        self.tde_filename = None
        self.incremental_refresh_field = None
        self.column_mapping = {}
        self.column_aliases = {}
        self.datasource_filters = []
        self.extract_filters = []
        self.initial_sql = None
        self.column_instances = []

    def add_first_table(self, db_table_name, table_alias):
        self.main_table_relation = self.create_table_relation(db_table_name, table_alias)

    def add_first_custom_sql(self, custom_sql, table_alias):
        self.main_table_relation = self.create_custom_sql_relation(custom_sql, table_alias)

    @staticmethod
    def create_random_calculation_name():
        n = 19
        range_start = 10 ** (n - 1)
        range_end = (10 ** n) - 1
        random_digits = random.randint(range_start, range_end)
        return 'Calculation_{}'.format(str(random_digits))

    @staticmethod
    def create_table_relation(db_table_name, table_alias):
        r = etree.Element("relation")
        r.set('name', table_alias)
        r.set("table", "[{}]".format(db_table_name))
        r.set("type", "table")
        return r

    @staticmethod
    def create_custom_sql_relation(custom_sql, table_alias):
        r = etree.Element("relation")
        r.set('name', table_alias)
        r.text = custom_sql
        r.set("type", "text")
        return r

    # on_clauses = [ { left_table_alias : , left_field : , operator : right_table_alias : , right_field : ,  },]
    @staticmethod
    def define_join_on_clause(left_table_alias, left_field, operator, right_table_alias, right_field):
        return {"left_table_alias": left_table_alias,
                "left_field": left_field,
                "operator": operator,
                "right_table_alias": right_table_alias,
                "right_field": right_field
                }

    def join_table(self, join_type, db_table_name, table_alias, join_on_clauses, custom_sql=None):
        full_join_desc = {"join_type": join_type.lower(),
                          "db_table_name": db_table_name,
                          "table_alias": table_alias,
                          "on_clauses": join_on_clauses,
                          "custom_sql": custom_sql}
        self.join_relations.append(full_join_desc)

    def generate_relation_section(self):
        # Because of the strange way that the interior definition is the last on, you need to work inside out
        # "Middle-out" as Silicon Valley suggests.
        # Generate the actual JOINs

        # There's only a single main relation with only one table
        if len(self.join_relations) == 0:
            self.connection.append(self.main_table_relation)
            self.ds_xml.append(self.connection)
        else:
            prev_relation = self.main_table_relation
            # We go through each relation, build the whole thing, then append it to the previous relation, then make
            # that the new prev_relationship. Something like recurssion
            for join_desc in self.join_relations:
                r = etree.Element("relation")
                r.set("join", join_desc["join_type"])
                r.set("type", "join")
                if len(join_desc["on_clauses"]) == 0:
                    raise InvalidOptionException("Join clause must have at least one ON clause describing relation")
                else:
                    and_expression = None
                    if len(join_desc["on_clauses"]) > 1:
                        and_expression = etree.Element("expression")
                        and_expression.set("op", 'AND')
                    for on_clause in join_desc["on_clauses"]:
                        c = etree.Element("clause")
                        c.set("type", "join")
                        e = etree.Element("expression")
                        e.set("op", on_clause["operator"])

                        e_field1 = etree.Element("expression")
                        e_field1_name = '[{}].[{}]'.format(on_clause["left_table_alias"],
                                                            on_clause["left_field"])
                        e_field1.set("op", e_field1_name)
                        e.append(e_field1)

                        e_field2 = etree.Element("expression")
                        e_field2_name = '[{}].[{}]'.format(on_clause["right_table_alias"],
                                                            on_clause["right_field"])
                        e_field2.set("op", e_field2_name)
                        e.append(e_field2)
                        if and_expression is not None:
                            and_expression.append(e)
                        else:
                            and_expression = e

                c.append(and_expression)
                r.append(c)
                r.append(prev_relation)

                if join_desc["custom_sql"] is None:
                    new_table_rel = self.create_table_relation(join_desc["db_table_name"],
                                                               join_desc["table_alias"])
                elif join_desc["custom_sql"] is not None:
                    new_table_rel = self.create_custom_sql_relation(join_desc['custom_sql'],
                                                                    join_desc['table_alias'])
                r.append(new_table_rel)
                prev_relation = r

                self.connection.append(prev_relation)
                self.ds_xml.append(self.connection)

    def add_table_column(self, table_alias, table_field_name, tableau_field_alias):
        # Check to make sure the alias has been added

        # Check to make sure the tableau_field_alias hasn't been used already

        self.column_mapping[tableau_field_alias] = "[{}].[{}]".format(table_alias, table_field_name)

    def add_column_alias(self, tableau_field_alias, caption=None, dimension_or_measure=None,
                         discrete_or_continuous=None, datatype=None, calculation=None):
        if dimension_or_measure.lower() in ['dimension', 'measure']:
            role = dimension_or_measure.lower()
        else:
            raise InvalidOptionException("{} should be either measure or dimension".format(dimension_or_measure))

        if discrete_or_continuous.lower() in ['discrete', 'continuous']:
            if discrete_or_continuous.lower() == 'discrete':
                if datatype.lower() in ['string']:
                    t_type = 'nominal'
                else:
                    t_type = 'ordinal'
            elif discrete_or_continuous.lower() == 'continuous':
                t_type = 'quantitative'
        else:
            raise InvalidOptionException("{} should be either discrete or continuous".format(discrete_or_continuous))

        if datatype.lower() not in ['string', 'integer', 'datetime', 'date', 'real', 'boolean']:
            raise InvalidOptionException("{} is not a valid datatype".format(datatype))

        self.column_aliases[tableau_field_alias] = {"caption": caption,
                                                    "type": t_type,
                                                    "datatype": datatype.lower(),
                                                    "role": role,
                                                    "calculation": calculation}

    def add_calculation(self, calculation, calculation_name, dimension_or_measure, discrete_or_continuous, datatype):
        internal_calc_name = self.create_random_calculation_name()
        self.add_column_alias(internal_calc_name, calculation_name, dimension_or_measure, discrete_or_continuous,
                              datatype, calculation)
        # internal_calc_name allows you to create a filter on this
        return internal_calc_name

    @staticmethod
    def create_dimension_filter(column_name, values, include_or_exclude='include', custom_value_list=False):
        # Check if column_name is actually the alias of a calc, if so, replace with the random internal calc name

        if include_or_exclude.lower() in ['include', 'exclude']:
            if include_or_exclude.lower() == 'include':
                ui_enumeration = 'inclusive'
            elif include_or_exclude.lower() == 'exclude':
                ui_enumeration = 'exclusive'
        else:
            raise InvalidOptionException('{} is not valid, must be include or exclude'.format(include_or_exclude))
        ds_filter = {
                     "type": 'categorical',
                     'column_name': '[{}]'.format(column_name),
                     "values": values,
                     'ui-enumeration': ui_enumeration,
                     'ui-manual-selection': custom_value_list
                    }
        return ds_filter

    def add_dimension_datasource_filter(self, column_name, values, include_or_exclude='include',
                                        custom_value_list=False):
        ds_filter = self.create_dimension_filter(column_name, values, include_or_exclude, custom_value_list)
        self.datasource_filters.append(ds_filter)

    def add_dimension_extract_filter(self, column_name, values, include_or_exclude='include', custom_value_list=False):
        ds_filter = self.create_dimension_filter(column_name, values, include_or_exclude, custom_value_list)
        self.extract_filters.append(ds_filter)

    def add_continuous_datasource_filter(self, column_name, min_value=None, max_value=None, date=False):
        ds_filter = self.create_continuous_filter(column_name, min_value=min_value, max_value=max_value, date=date)
        self.datasource_filters.append(ds_filter)

    def add_continuous_extract_filter(self, column_name, min_value=None, max_value=None, date=False):
        ds_filter = self.create_continuous_filter(column_name, min_value=min_value, max_value=max_value ,date=date)
        self.extract_filters.append(ds_filter)

    def add_relative_date_datasource_filter(self, column_name, period_type, number_of_periods=None,
                                            previous_next_current='previous', to_date=False):
        ds_filter = self.create_relative_date_filter(column_name, period_type, number_of_periods, previous_next_current
                                                     , to_date)
        self.datasource_filters.append(ds_filter)

    def add_relative_date_extract_filter(self, column_name, period_type, number_of_periods=None,
                                         previous_next_current='previous', to_date=False):
        ds_filter = self.create_relative_date_filter(column_name, period_type, number_of_periods, previous_next_current,
                                                     to_date)
        self.extract_filters.append(ds_filter)

    def create_continuous_filter(self, column_name, min_value=None, max_value=None, date=False):
        # Dates need to be wrapped in # #
        if date is True:
            if min_value is not None:
                min_value = '#{}#'.format(str(min_value))
            if max_value is not None:
                max_value = '#{}#'.format(str(max_value))
            final_column_name = '[none:{}:qk]'.format(column_name)
            # Need to create a column-instance tag in the columns section
            self.column_instances.append(
                {
                    'column': '[{}]'.format(column_name),
                    'name': final_column_name,
                    'type': 'quantitative'
                }
            )
        else:
            final_column_name = '[{}]'.format(column_name)
        ds_filter = {
            'type': 'quantitative',
            'min': min_value,
            'max': max_value,
            'column_name': final_column_name,
        }
        return ds_filter

    def create_relative_date_filter(self, column_name, period_type, number_of_periods,
                                    previous_next_current='previous', to_date=False):
        if period_type.lower() not in ['quarter', 'year', 'month', 'day', 'hour', 'minute']:
            raise InvalidOptionException('period_type must be one of : quarter, year, month, day, hour, minute')
        # Need to create a column-instance tag in the columns section
        final_column_name = '[none:{}:qk]'.format(column_name)
        # Need to create a column-instance tag in the columns section
        self.column_instances.append(
            {
                'column': '[{}]'.format(column_name),
                'name': final_column_name,
                'type': 'quantitative'
            }
        )
        if previous_next_current.lower() == 'previous':
            first_period = '-{}'.format(str(number_of_periods))
            last_period = '0'
        elif previous_next_current.lower() == 'next':
            first_period = '1'
            last_period = '{}'.format(str(number_of_periods))
        elif previous_next_current.lower() == 'current':
            first_period = '0'
            last_period = '0'
        else:
            raise InvalidOptionException('You must use "previous", "next" or "current" for the period selections')

        if to_date is False:
            include_future = 'true'
        elif to_date is True:
            include_future = 'false'

        ds_filter = {
            'type': 'relative-date',
            'column_name': final_column_name,
            'first-period': first_period,
            'last-period': last_period,
            'period-type': period_type.lower(),
            'include-future': include_future
        }
        return ds_filter

    def generate_filters(self, filter_array):
        return_array = []
        for filter_def in filter_array:
            f = etree.Element('filter')
            f.set('class', filter_def['type'])
            f.set('column', filter_def['column_name'])
            f.set('filter-group', '2')
            if filter_def['type'] == 'quantitative':
                f.set('include-values', 'in-range')
                if filter_def['min'] is not None:
                    m = etree.Element('min')
                    m.text = str(filter_def['min'])
                    f.append(m)
                if filter_def['max'] is not None:
                    m = etree.Element('max')
                    m.text = str(filter_def['max'])
                    f.append(m)
            elif filter_def['type'] == 'relative-date':
                f.set('first-period', filter_def['first-period'])
                f.set('include-future', filter_def['include-future'])
                f.set('last-period', filter_def['last-period'])
                f.set('include-null', 'false')
                f.set('period-type', filter_def['period-type'])

            elif filter_def['type'] == 'categorical':
                gf = etree.Element('groupfilter')
                # This attribute has a user namespace
                gf.set('{' + '{}'.format(self.nsmap['user']) + '}ui-domain', 'database')
                gf.set('{' + '{}'.format(self.nsmap['user']) + '}ui-enumeration', filter_def['ui-enumeration'])
                gf.set('{' + '{}'.format(self.nsmap['user']) + '}ui-marker', 'enumerate')
                if filter_def['ui-manual-selection'] is True:
                    gf.set('{' + '{}'.format(self.nsmap['user']) + '}ui-manual-selection', 'true')
                if len(filter_def['values']) == 1:
                    if filter_def['ui-enumeration'] == 'exclusive':
                        gf.set('function', 'except')
                        gf1 = etree.Element('groupfilter')
                        gf1.set('function', 'member')
                        gf1.set('level', filter_def['column_name'])
                    else:
                        gf.set('function', 'member')
                        gf.set('level', filter_def['column_name'])
                        gf1 = gf
                    # strings need the &quot;, ints do not
                    if isinstance(filter_def['values'][0], str):
                        gf1.set('member', quoteattr(filter_def['values'][0]))
                    else:
                        gf1.set('member', str(filter_def['values'][0]))
                    if filter_def['ui-enumeration'] == 'exclusive':
                        # Single exclude filters include an extra groupfilter set with level-members function
                        lm = etree.Element('groupfilter')
                        lm.set('function', 'level-members')
                        lm.set('level', filter_def['column_name'])
                        gf.append(lm)
                        gf.append(gf1)
                    f.append(gf)
                else:
                    if filter_def['ui-enumeration'] == 'exclusive':
                        gf.set('function', 'except')
                    else:
                        gf.set('function', 'union')
                    for val in filter_def['values']:
                        gf1 = etree.Element('groupfilter')
                        gf1.set('function', 'member')
                        gf1.set('level', filter_def['column_name'])
                        # String types need &quot; , ints do not
                        if isinstance(val, str):
                            gf1.set('member', quoteattr(val))
                        else:
                            gf1.set('member', str(val))
                        gf.append(gf1)
                    f.append(gf)
            return_array.append(f)
        return return_array

    def generate_datasource_filters_section(self):
        filters = self.generate_filters(self.datasource_filters)
        filters_array = []
        for f in filters:
            filters_array.append(f)
        return filters_array

    def generate_cols_map_section(self):
        if len(self.column_mapping) == 0:
            return False
        c = etree.Element("cols")
        for key in self.column_mapping:
            m = etree.Element("map")
            m.set("key", "[{}]".format(key))
            m.set("value", self.column_mapping[key])
            c.append(m)
        self.ds_xml.append(c)

    @staticmethod
    def generate_aliases_tag():
        # For whatever reason, the aliases tag does not contain the columns, but it always precedes it
        a = etree.Element("aliases")
        a.set("enabled", "yes")
        return a

    def generate_aliases_column_section(self):
        column_aliases_array = []

        # Now to put in each column tag
        for column_alias in self.column_aliases:
            c = etree.Element("column")
            # Name is the Tableau Field Alias, always surrounded by brackets SQL Server style
            c.set("name", "[{}]".format(column_alias))
            if self.column_aliases[column_alias]["datatype"] is not None:
                c.set("datatype", self.column_aliases[column_alias]["datatype"])
            if self.column_aliases[column_alias]["caption"] is not None:
                c.set("caption", self.column_aliases[column_alias]["caption"])
            if self.column_aliases[column_alias]["role"] is not None:
                c.set("role", self.column_aliases[column_alias]["role"])
            if self.column_aliases[column_alias]["type"] is not None:
                c.set("type", self.column_aliases[column_alias]["type"])
            if self.column_aliases[column_alias]['calculation'] is not None:
                calc = etree.Element('calculation')
                calc.set('class', 'tableau')
                # quoteattr adds an extra real set of quotes around the string, which needs to be sliced out
                calc.set('formula', quoteattr(self.column_aliases[column_alias]['calculation'])[1:-1])
                c.append(calc)
            column_aliases_array.append(c)
        return column_aliases_array

    def generate_column_instances_section(self):
        column_instances_array = []
        for column_instance in self.column_instances:
            ci = etree.Element('column-instance')
            ci.set('column', column_instance['column'])
            ci.set('derivation', 'None')
            ci.set('name', column_instance['name'])
            ci.set('pivot', 'key')
            ci.set('type', column_instance['type'])
            column_instances_array.append(ci)
        return column_instances_array

    def generate_extract_section(self):
        # Short circuit if no extract had been set
        if self.tde_filename is None:
            self.log('No tde_filename, no extract being added')
            return False
        self.log('Importing the Tableau SDK to build the extract')

        # Import only if necessary
        self.log('Building the extract Element object')
        from .tde_file_generator import TDEFileGenerator
        e = etree.Element('extract')
        e.set('count', '-1')
        e.set('enabled', 'true')
        e.set('units', 'records')

        c = etree.Element('connection')
        c.set('class', 'dataengine')
        c.set('dbname', 'Data/Datasources/{}'.format(self.tde_filename))
        c.set('schema', 'Extract')
        c.set('tablename', 'Extract')
        right_now = datetime.datetime.now()
        pretty_date = right_now.strftime("%m/%d/%Y %H:%M:%S %p")
        c.set('update-time', pretty_date)

        r = etree.Element('relation')
        r.set("name", "Extract")
        r.set("table", "[Extract].[Extract]")
        r.set("type", "table")
        c.append(r)

        calcs = etree.Element("calculations")
        calc = etree.Element("calculation")
        calc.set("column", "[Number of Records]")
        calc.set("formula", "1")
        calcs.append(calc)
        c.append(calcs)

        ref = etree.Element('refresh')
        if self.incremental_refresh_field is not None:
            ref.set("increment-key", self.incremental_refresh_field)
            ref.set("incremental-updates", 'true')
        elif self.incremental_refresh_field is None:
            ref.set("increment-key", "")
            ref.set("incremental-updates", 'false')

        c.append(ref)

        e.append(c)

        tde_columns = {}
        self.log('Creating the extract filters')
        if len(self.extract_filters) > 0:
            filters = self.generate_filters(self.extract_filters)
            for f in filters:
                e.append(f)
            # Any column in the extract filters needs to exist in the TDE file itself
            if len(self.extract_filters) > 0:
                for f in self.extract_filters:
                    # Check to see if column_name is actually an instance
                    field_name = f['column_name']
                    for col_instance in self.column_instances:
                        if field_name == col_instance['name']:
                            field_name = col_instance['column']
                    # Simple heuristic for determining type from the first value in the values array
                    if f['type'] == 'categorical':
                        first_value = f['values'][0]
                        if isinstance(first_value, str):
                            filter_column_tableau_type = 'str'
                        else:
                            filter_column_tableau_type = 'int'
                    elif f['type'] == 'relative-date':
                        filter_column_tableau_type = 'datetime'
                    elif f['type'] == 'quantitative':
                        # Time values passed in with strings
                        if isinstance(f['max'], str) or isinstance(f['min'], str):
                            filter_column_tableau_type = 'datetime'
                        else:
                            filter_column_tableau_type = 'int'
                    else:
                        raise InvalidOptionException('{} is not a valid type'.format(f['type']))
                    tde_columns[field_name[1:-1]] = filter_column_tableau_type
        else:
            self.log('Creating TDE with only one field, "Generic Field", of string type')
            tde_columns['Generic Field'] = 'str'

        self.log('Using the Extract SDK to build an empty extract file with the right definition')
        tde_file_generator = TDEFileGenerator(self.logger)
        tde_file_generator.set_table_definition(tde_columns)
        tde_file_generator.create_tde(self.tde_filename)
        return e

    def get_xml_string(self):
        self.generate_relation_section()
        self.generate_cols_map_section()

        # Column Aliases
        cas = self.generate_aliases_column_section()
        # Need Aliases tag if there are any column tags
        if len(cas) > 0:
            self.ds_xml.append(self.generate_aliases_tag())
        for c in cas:
            self.log('Appending the column alias XML')
            self.ds_xml.append(c)
        # Column Instances
        cis = self.generate_column_instances_section()
        for ci in cis:
            self.log('Appending the column-instances XML')
            self.ds_xml.append(ci)
        # Data Source Filters
        dsf = self.generate_datasource_filters_section()
        for f in dsf:
            self.log('Appending the datasource filters XML')
            self.ds_xml.append(f)
        # Extract
        e = self.generate_extract_section()
        if e is not False:
            self.log('Appending the extract XML')
            self.ds_xml.append(e)

        xmlstring = etree.tostring(self.ds_xml, pretty_print=True, xml_declaration=True, encoding='utf-8')
        self.log(xmlstring)
        return xmlstring

    def save_file(self, filename_no_extension, save_to_directory):
        self.start_log_block()
        file_extension = '.tds'
        if self.tde_filename is not None:
            file_extension = '.tdsx'
        try:
            tds_filename = filename_no_extension + '.tds'
            lh = open(save_to_directory + tds_filename, 'wb')
            lh.write(self.get_xml_string())
            lh.close()

            if file_extension == '.tdsx':
                zf = zipfile.ZipFile(save_to_directory + filename_no_extension + '.tdsx', 'w')
                zf.write(save_to_directory + tds_filename, '/{}'.format(tds_filename))
                # Delete temporary TDS at some point
                zf.write(self.tde_filename, '/Data/Datasources/{}'.format(self.tde_filename))
                zf.close()
                # Remove the temp tde_file that is created
                os.remove(self.tde_filename)
        except IOError:
            self.log("Error: File '{} cannot be opened to write to".format(filename_no_extension + file_extension))
            self.end_log_block()
            raise

    def add_extract(self, tde_filename, incremental_refresh_field=None):
        self.tde_filename = tde_filename
        self.incremental_refresh_field = incremental_refresh_field


class TableauParametersGenerator(TableauBase):
    def __init__(self, logger_obj):
        super(self.__class__, self).__init__()
        self.logger = logger_obj
        self.nsmap = {"user": 'http://www.tableausoftware.com/xml/user'}
        self.ds_xml = etree.Element("datasource")
        self.ds_xml.set('name', 'Parameters')
        # Initialization of the datasource
        self.ds_xml.set('hasconnection', 'false')
        self.ds_xml.set('inline', 'true')
        a = etree.Element('aliases')
        a.set('enabled', 'yes')
        self.ds_xml.append(a)

        self.parameters = []

    def add_parameter(self, name, datatype, allowable_values, current_value, values_list=None, range_dict=None):
        if datatype.lower() not in ['string', 'integer', 'datetime', 'date', 'real', 'boolean']:
            raise InvalidOptionException("{} is not a valid datatype".format(datatype))
        if allowable_values not in ['all', 'list', 'range']:
            raise InvalidOptionException("{} is not valid allowable_values option. Only 'all', 'list' or 'range'")

        # range_dict = { min: None, max: None, step_size: None, period_type: None}

        param_dict = {
                        'allowable_values': allowable_values,
                        'datatype': datatype,
                        'current_value': current_value,
                        'values_list': values_list,
                        'range_dict': range_dict,
                        'caption': name
        }
        self.parameters.append(param_dict)

    @staticmethod
    def create_parameter_column(param_number, param_dict):
        c = etree.Element("column")
        c.set('caption', param_dict['caption'])
        c.set('name', '[Parameter {}]'.format(str(param_number)))
        c.set('param-domain-type', param_dict['allowable_values'])
        if param_dict['datatype'] in ['integer', 'real']:
            c.set('type', 'quantitative')
        else:
            c.set('type', 'nominal')
        c.set('role', 'measure')
        c.set('datatype', param_dict['datatype'])

        # Range
        if param_dict['allowable_values'] == 'range':
            r = etree.Element('range')
            if param_dict['range_dict']['max'] is not None:
                r.set('max', str(param_dict['range_dict']['max']))
            if param_dict['range_dict']['min'] is not None:
                r.set('min', str(param_dict['range_dict']['min']))
            if param_dict['range_dict']['step_size'] is not None:
                r.set('granularity', str(param_dict['range_dict']['step_size']))
            if param_dict['range_dict']['period_type'] is not None:
                r.set('period-type', str(param_dict['range_dict']['period_type']))
            c.append(r)

        # List
        aliases = None
        if param_dict['allowable_values'] == 'list':
            members = etree.Element('members')

            for value_pair in param_dict['values_list']:
                for value in value_pair:
                    member = etree.Element('member')
                    member.set('value', str(value))
                    if value_pair[value] is not None:
                        if aliases is None:
                            aliases = etree.Element('aliases')
                        alias = etree.Element('alias')
                        alias.set('key', str(value))
                        alias.set('value', str(value_pair[value]))
                        member.set('alias', str(value_pair[value]))
                        aliases.append(alias)
                    members.append(member)
                if aliases is not None:
                    c.append(aliases)
                c.append(members)

        # If you have aliases, then need to put alias in the alias parameter, and real value in the value parameter
        if aliases is not None:
            c.set('alias', str(param_dict['current_value']))
            # Lookup the actual value of the alias
            for value_pair in param_dict['values_list']:
                for value in value_pair:
                    if value_pair[value] == param_dict['current_value']:
                        actual_value = value
        else:
            actual_value = param_dict['current_value']

        if isinstance(param_dict['current_value'], str) and param_dict['datatype'] not in ['date', 'datetime']:
            c.set('value', quoteattr(actual_value))
        else:
            c.set('value', str(actual_value))

        calc = etree.Element('calculation')
        calc.set('class', 'tableau')
        if isinstance(param_dict['current_value'], str) and param_dict['datatype'] not in ['date', 'datetime']:
            calc.set('formula', quoteattr(actual_value))
        else:
            calc.set('formula', str(actual_value))
        c.append(calc)

        return c

    def get_xml_string(self):
        i = 1
        for parameter in self.parameters:
            c = self.create_parameter_column(i, parameter)
            self.ds_xml.append(c)
            i += 1

        xmlstring = etree.tostring(self.ds_xml, pretty_print=True, xml_declaration=False, encoding='utf-8')
        self.log(xmlstring)
        return xmlstring
