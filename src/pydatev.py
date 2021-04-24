# -*- coding: utf-8 -*-
#
# A python module to import and export DATEV files.
# Author: Frank Stollmeier
# License: GNU GPLv3
#

import os
import datetime 
from collections import UserDict
import pickle
import pkg_resources
try:
    import pandas as pd
except ImportError:
    pass

class DatevFormatError(ValueError):
    '''Error for everything that conflicts with the DATEV file format specifications.'''
    pass

with pkg_resources.resource_stream(__name__, "format-specifications.dat") as f:
    specifications = pickle.load(f)


class DatevEntry(UserDict):
    '''A generic class for entries that are part of one of the data categories. The classes for entries of a specific data category should inherit from this class.
    An instance of this class bahaves almost like a dictionary, but instead of arbitrary keys, only specific keys are allowed, and instead of arbitrary datatypes for the values, only specific datatypes are allowed.'''

    def __init__(self, fields):
        super().__init__()
        self._fields = fields
        self._labels = [f['Label'] for f in fields]
        self._aliases = dict([(f['LabelAlias'],f['Label']) for f in fields if ('LabelAlias' in f and not f['LabelAlias'] is None)])
        self._fields_dict = dict([(field['Label'],field) for field in fields])
        for field in self._fields:
            self[field['Label']] = None
        
    def __setitem__(self, key, value):
        #check if key is valid
        if key in self._aliases:
            key = self._aliases[key]
        if not key in self._labels:
            raise KeyError("Adding new keys is not allowed.")
        #check if datatype of value is valid
        format_type = self._fields_dict[key]['FormatType']
        decimal_places = int(self._fields_dict[key]['DecimalPlaces'])
        if not value is None:
            if format_type == 'Betrag':
                if not isinstance(value, float):
                    raise DatevFormatError("The value for key '{}' needs to be of type float.".format(key))
            elif format_type == 'Datum':
                if not type(value) is datetime.date: #Check with type(), because isinstance() would also accept datetime.datetime.
                    raise DatevFormatError("The value for key '{}' needs to be of type datetime.date.".format(key))
            elif format_type == 'Datum JJJJMMTT':
                if not type(value) is datetime.date: #Check with type(), because isinstance() would also accept datetime.datetime.
                    raise DatevFormatError("The value for key '{}' needs to be of type datetime.date.".format(key))
            elif format_type == 'Konto':
                if not isinstance(value, str):
                    raise DatevFormatError("The value for key '{}' needs to be of type str.".format(key))
                if not value.isdigit():
                    raise DatevFormatError("The value for key '{}' needs to be a string of digits.".format(key))
            elif format_type == 'Text':
                if not isinstance(value, str):
                    raise DatevFormatError("The value for key '{}' needs to be of type str.".format(key))
            elif format_type == 'Zahl' and decimal_places == 0:
                if not isinstance(value, int):
                    raise DatevFormatError("The value for key '{}' needs to be of type int.".format(key))
            elif format_type == 'Zahl' and decimal_places > 0:
                if not isinstance(value, float):
                    raise DatevFormatError("The value for key '{}' needs to be of type float.".format(key))
            elif format_type == 'Zeitstempel':
                if not isinstance(value, datetime.datetime):
                    raise DatevFormatError("The value for key '{}' needs to be of type datetime.datetime.".format(key))
            else:
                raise NotImplementedError("Unknown FormatType: {}".format(format_type))
        
        #set value
        super().__setitem__(key, value)
    
    def __str__(self):
        '''Show the date of the entry that is set, but not the fields that are set to None.'''
        s = '{'
        for label in self._labels:
            if self[label] is None:
                continue
            s += "{}: {}, ".format(label, self[label])
        s = s[:-2] + '}'
        return s
    
    def verify(self):
        '''Check whether all required fields are filled.'''
        missing = []
        for field in self._fields:
            if int(field['Necessary']) == 1:
                if self[field['Label']] is None:
                    missing.append(field['Label'])
        if len(missing) > 0:
            raise DatevFormatError('The following necessary values are missing: ' + str(missing))
        return True
    
    def python2datev(self, key):
        '''Return value in datev format.'''
        value = self[key]
        format_type = self._fields_dict[key]['FormatType']
        length = -1 if self._fields_dict[key]['Length'] is None else int(self._fields_dict[key]['Length'])
        decimal_places = int(self._fields_dict[key]['DecimalPlaces'])
        max_length = length + 1 + decimal_places if format_type in ['Betrag','Zahl'] else length
        
        if value is None:
            if format_type == 'Text':
                s = '""'
            else:
                s = ''
        else:
            if format_type == 'Betrag':
                s = '{:.{}f}'.format(value, decimal_places).replace('.',',')
            elif format_type == 'Datum':
                if length == 4:
                    s = "{:0>2d}".format(value.day) + "{:0>2d}".format(value.month)
                elif length == 8:
                    s = "{:0>2d}".format(value.day) + "{:0>2d}".format(value.month) + "{:0>4d}".format(value.year) 
                else:
                    raise NotImplementedError("Unknown date format.")
            elif format_type == 'Datum JJJJMMTT':
                s = "{:0>4d}".format(value.year) + "{:0>2d}".format(value.month) + "{:0>2d}".format(value.day)
                
            elif format_type == 'Konto':
                s = value
                
            elif format_type == 'Text':
                 s = '"' + value + '"'
                
            elif format_type == 'Zahl':
                s =  '{:.{}f}'.format(value, decimal_places).replace('.',',')
            
            elif format_type == 'Zeitstempel':
                s = "{:0>4d}{:0>2d}{:0>2d}{:0>2d}{:0>2d}{:0>2d}{:0>3d}".format(value.year,value.month,value.day,value.hour,value.minute,value.second,value.microsecond)
            
            else:
                raise NotImplementedError("Unknown FormatType: {}".format(format_type))
        
        if length > 0:
            if len(s.replace('"','')) > max_length:
                raise DatevFormatError("The value {} has {} characters, but the DATEV file specification allows only {} characters for values at key {}.".format(s, len(s), max_length, key))
        
        return s

    def serialize(self):
        '''Convert data to a string as it is represented in a DATEV file. For the inverse operation, see self.parse().'''
        parts = [self.python2datev(field['Label']) for field in self._fields]
        return ';'.join(parts)
    
    def datev2python(self, key, string, year = None):
        '''Parse a string containing a single datum from a DATEV-file and save the content as a python datatype in self[key].
        
        Parameters
        ----------
        key:    str, the field identifier
        string: str, a single datum from a DATEV file  
        year:   int, only required if the string contains a date
        '''
        format_type = self._fields_dict[key]['FormatType']
        length = -1 if self._fields_dict[key]['Length'] is None else int(self._fields_dict[key]['Length'])
        decimal_places = int(self._fields_dict[key]['DecimalPlaces'])
        max_length = length + 1 + decimal_places if format_type in ['Betrag','Zahl'] else length
        
        if len(string) == 0 or string == '""':
            value = None
        elif format_type == 'Betrag':
            value = float(string.replace(',', '.')) 
        elif format_type == 'Datum':
            if length == 8 and ('FormatExpression' in self._fields_dict[key]) and (self._fields_dict[key]['FormatExpression'] == 'TTMM'):
                value = datetime.date(year, int(string[2:4]), int(string[0:2]))
            elif length == 8 and len(string) == 8:
                value = datetime.date(int(string[4:8]), int(string[2:4]), int(string[0:2]))  
            else:
                raise NotImplementedError("Unknown date format.")
        elif format_type == 'Datum JJJJMMTT':
            value = datetime.date(int(string[0:4]), int(string[4:6]), int(string[6:8]))  
        elif format_type == 'Konto':
            value = string
        elif format_type == 'Text':
            value = string.replace('"','')
        elif format_type == 'Zahl':
            if decimal_places == 0:
                value = int(string)
            elif decimal_places > 0:
                value = float(string.replace(',', '.')) 
        elif format_type == 'Zeitstempel':
            t = string
            value = datetime.datetime(int(t[:4]),int(t[4:6]),int(t[6:8]),int(t[8:10]),int(t[10:12]),int(t[12:14]),int(t[14:17]))
        self[key] = value
    
    def parse(self, line, year = None):
        '''Read a string of one line from a DATEV file, convert the content to python datatypes and store the results. For the inverse operation, see self.serialize().
        
        Parameters
        ----------
        line:   str
        year:   int
        '''
        values = line.split(';')
        if not len(values) == len(self._labels):
            raise IOError("Unable to parse line: " + line)
        for field,value in zip(self._fields, values):
            self.datev2python(field['Label'], value, year)
    
    @property
    def required_keys(self):
        return [field['Label'] for field in self._fields if int(field['Necessary']) == 1]



class DatevDataCategory(object):
    '''This is the base class for Datev data categories. Each data category should inherit from this class.''' 
    
    def __init__(self, category_type, version):
        self._category_type = category_type
        self._version = str(version)
        if not category_type in specifications:
            raise ValueError("Unknown category_type: " + category_type)
        if not self._version in specifications[category_type]:
            raise ValueError("Version {} unknown for category {}".format(self._version, category_type))
        self._metadata = DatevEntry(specifications['Metadaten']['Andere']['Field'])
        self._data = []
        
    def load(self, filename):
        '''Load a datev file.
        
        Parameters
        ----------
        filename:       string
        '''
        with open(filename, 'r', encoding = 'ISO-8859-1') as f:
            content = f.read().splitlines()
        header_line = content[0]
        column_line = content[1]
        entry_lines = content[2:]
        
        self._metadata.parse(header_line)
        self.parse_data(column_line, entry_lines)
        
    def save(self, filename):
        '''Save data to a Datev file. The Datev file specification require that the filename has the format EXTF_<arbitrary-name>.csv, e.g. EXTF_Buchungsstapel__<date_time>_<export number>.csv .
        
        Parameters
        ----------
        filename:    string
        '''
        fn = os.path.split(filename)[1]
        if not fn[:5] == 'EXTF_' or not os.path.splitext(fn)[1] == '.csv':
            raise DatevFormatError("The Datev file specification require that the filename has the format EXTF_<arbitrary-name>.csv, e.g. EXTF_Buchungsstapel__<date_time>_<export number>.csv .")
        with open(filename, 'w', encoding = 'ISO-8859-1') as f:
            f.write(self._metadata.serialize() + '\n')
            f.write(self.serialize_data())
    
    @property
    def data(self):
        return self._data
    
    @property
    def metadata(self):
        return self._metadata
    
    def add_entry(self):
        new_entry = DatevEntry(specifications[self._category_type][self._version]['Field'])
        self._data.append(new_entry)
        return new_entry
    
    def parse_data(self, column_line, entry_lines):
        '''Parse the body of a datev file. 
        
        Parameters
        ----------
        column_line:    string
        entry_lines:    list of strings
        '''
        for line in entry_lines:
            new_entry = self.add_entry()
            new_entry.parse(line, year = self._metadata['Wirtschaftsjahr-Beginn'].year)
            
    
    def serialize_data(self):
        '''Serialize the data of the body of a datev file. 
        '''
        lines = []
        #header
        first_entry = self._data[0]
        lines.append(';'.join(first_entry.keys())) 
        #body
        for entry in self._data:
            lines.append(entry.serialize())
        return '\n'.join(lines)
    
    def export_as_pandas_dataframe(self):
        '''Return data as a pandas DataFrame.'''
        data = []
        for entry in self._data:
            e = []
            for key in entry.keys():
                e.append(entry[key])
            data.append(e)
        try:   
            return pd.DataFrame(data, columns = self._data[0].keys())
        except NameError:
            raise RuntimeError("You need to install the python module 'pandas' to use this function.")
    
    def verify(self):
        '''Check wheter metadata and all entries are valid.'''
        errors = []
        if not self._metadata['DATEV-Format-KZ'] in ['DTVF','EXTF']:
            errors.append("Metadata: DATEV-Format-KZ needs to be either DTVF or EXTF, but not " + str(self._metadata['DATEV-Format-KZ']))
        try:
            self._metadata.verify()
        except DatevFormatError as dfe:
            errors.append("Metadata: ", dfe.args[0])
        for i,entry in enumerate(self._data):
            try:
                entry.verify()
            except DatevFormatError as dfe:
                errors.append("Entry {}: {}".format(i,dfe.args[0]))
        if len(errors) == 0:
            return True
        else:
            raise DatevFormatError("Invalid data.", errors)
        


class Buchungsstapel(DatevDataCategory):
    '''Datev Buchungsstapel'''
   
    def __init__(self, filename = None, berater = None, mandant = None, wirtschaftsjahr_beginn = None, sachkontennummernlänge = None, datum_von = None, datum_bis = None, waehrungskennzeichen = None, version = 9):
        '''If you specify the filename, the data will be loaded from there and the other parameters of this functions are ignored. If you don't specify the filename, a new empty Buchungsstapel will be created using the metadata of the other parameters. 
        
        Parameters
        ----------
        filename:               str
        berater:                int
        mandant:                int
        wirtschaftsjahr_beginn: datetime.date
        sachkontennummernlänge: int
        datum_von:              datetime.date
        datum_bis:              datetime.date
        waehrungskennzeichen:   str, optional, but recommended
        version:                int, optional
        '''
        super().__init__("Buchungsstapel", version)
        self._metadata = DatevEntry(specifications['Metadaten']['Buchungsstapel']['Field'])
        if filename is None:
            if not wirtschaftsjahr_beginn <= datum_von < datum_bis < datetime.date(wirtschaftsjahr_beginn.year+1,wirtschaftsjahr_beginn.month,wirtschaftsjahr_beginn.day):
                raise DatevFormatError("The dates datum_von and datum_bis should be between wirtschaftsjahr_beginn and wirtschaftsjahr_beginn + 1 year.")  
            if not 4 <= sachkontennummernlänge <= 9:
                raise DatevFormatError("The sachkontennummernlänge needs to be between 4 and 9.")
            if not 1 <= mandant <= 99999:
                raise DatevFormatError("The mandant number needs to be between 1 and 99999.")
            if not 1001 <= berater <= 9999999:
                raise DatevFormatError("The berater number needs to be between 1001 and 9999999.")
            self._metadata['DATEV-Format-KZ'] = 'EXTF'
            self._metadata['Versionsnummer'] = 700
            self._metadata['Datenkategorie'] = 21
            self._metadata['Formatname'] = 'Buchungsstapel'
            self._metadata['Formatversion'] = 9
            self._metadata['Berater'] = berater
            self._metadata['Mandant'] = mandant
            self._metadata['Wirtschaftsjahr-Beginn'] = wirtschaftsjahr_beginn
            self._metadata['Sachkontennummernlänge'] = sachkontennummernlänge
            self._metadata['Datum von'] = datum_von
            self._metadata['Datum bis'] = datum_bis
            self._metadata['Währungskennzeichen'] = waehrungskennzeichen
        else:
            self.load(filename)
    
    def add_entry(self):
        new_entry = super().add_entry()
        new_entry['WKZ Umsatz'] = self._metadata['Währungskennzeichen'] #set default value
        return new_entry
    
    def add_buchung(self, umsatz = None, soll_haben = None, konto = None, gegenkonto = None, belegdatum = None):
        '''Add Buchung to the batch. '''
        if len(self._data) == 99999:
            raise DatevFormatError("Datev file specification doesn't allow more than 99999 entries.")
        entry = self.add_entry()
        entry['Umsatz (ohne Soll/Haben-Kz)'] = umsatz
        entry['Soll/Haben-Kennzeichen'] = soll_haben
        entry['Kontonummer'] = konto
        entry['Gegenkonto (ohne BU-Schlüssel)'] = gegenkonto
        entry['Belegdatum'] = belegdatum
        return entry
    
    def verify(self):
        '''Check if all file format specifications are satisfied.'''
        errors = [] 
        try:
            super().verify()
        except DatevFormatError as dfe:
            errors.extend(dfe.args[1])
        for i,entry in enumerate(self._data):
            if not self._metadata['Datum von'] <= entry['Belegdatum'] <= self._metadata['Datum bis']:
                errors.append("The <Belegdatum> of Buchung {} is outside the specified time frame of this Buchungsstapel (from {} to {}).".format(i,str(self._metadata['Datum von']),str(self._metadata['Datum bis'])))
        if len(errors) > 0:
            raise DatevFormatError("Invalid data.", errors)
        else:
            return True




