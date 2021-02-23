# -*- coding: utf-8 -*-
#
# A python module to import and export DATEV files.
# Author: Frank Stollmeier
# License: GNU GPLv3
#

import os
import datetime 
from collections import UserDict
import pandas as pd
import pickle

with open('./pyDATEV/format-specifications.dat', 'rb') as f:
    specifications = pickle.load(f)

#collection of functions to convert from Datev format to python datatypes
conv_datev2python = dict()
conv_datev2python['int'] = lambda num: int(num)
conv_datev2python['float .2'] = lambda num: float(num.replace(',', '.')) 
conv_datev2python['float .4'] = lambda num: float(num.replace(',', '.')) 
conv_datev2python['float .6'] = lambda num: float(num.replace(',', '.')) 
conv_datev2python['text'] = lambda s: s.replace('"','')
conv_datev2python['account'] = lambda s: s
conv_datev2python['date DDMM'] = lambda DDMM, year: datetime.date(year, int(DDMM[2:4]), int(DDMM[0:2]))
conv_datev2python['date YYYYMMDD'] = lambda YYYYMMDD: datetime.date(int(YYYYMMDD[:4]), int(YYYYMMDD[4:6]), int(YYYYMMDD[6:8])) 
conv_datev2python['date DDMMYYYY'] = lambda DDMMYYYY: datetime.date(int(DDMMYYYY[4:8]), int(DDMMYYYY[2:4]), int(DDMMYYYY[0:2]))  
conv_datev2python['datetime YYYYMMDDhhmmssttt'] = lambda t: datetime.datetime(int(t[:4]),int(t[4:6]),int(t[6:8]),int(t[8:10]),int(t[10:12]),int(t[12:14]),int(t[14:17]))

#collection of functions to convert from python datatypes to Datev format
conv_python2datev = dict()
conv_python2datev['int'] = lambda num: str(num)
conv_python2datev['float .2'] = lambda number: '{:.2f}'.format(number).replace('.',',')
conv_python2datev['float .4'] = lambda number: '{:.4f}'.format(number).replace('.',',')
conv_python2datev['float .6'] = lambda number: '{:.6f}'.format(number).replace('.',',')
conv_python2datev['text'] = lambda string: '"' + string + '"'
conv_python2datev['account'] = lambda string: string
conv_python2datev['date DDMM'] = lambda date: "{:0>2d}".format(date.day) + "{:0>2d}".format(date.month)
conv_python2datev['date YYYYMMDD'] = lambda date: "{:0>4d}".format(date.year) + "{:0>2d}".format(date.month) + "{:0>2d}".format(date.day)
conv_python2datev['date DDMMYYYY'] = lambda date: "{:0>2d}".format(date.day) + "{:0>2d}".format(date.month) + "{:0>4d}".format(date.year) 
conv_python2datev['datetime YYYYMMDDhhmmssttt'] = lambda t: "{:0>4d}{:0>2d}{:0>2d}{:0>2d}{:0>2d}{:0>2d}{:0>3d}".format(t.year,t.month,t.day,t.hour,t.minute,t.second,t.microsecond)

#mapping of python datatypes and Datev formats (returned by conv_datev2python functions and accepted as parameter by conf_python2datev functions)
datatype = dict()
datatype['int'] = int
datatype['float .2'] = float
datatype['float .4'] = float
datatype['float .6'] = float
datatype['text'] = str
datatype['account'] = str
datatype['date DDMM'] = datetime.date
datatype['date DDMMYYYY'] = datetime.date
datatype['date YYYYMMDD'] = datetime.date
datatype['datetime YYYYMMDDhhmmssttt'] = datetime.datetime

class DatevEntry(UserDict):
    '''A generic class for entries that are part of one of the data categories. The classes for entries of a specific data category should inherit from this class.
    An instance of this class bahaves almost like a dictionary, but instead of arbitrary keys, only specific keys are allowed, and instead of arbitrary datatypes for the values, only specific datatypes are allowed.'''

    def __init__(self, keys, required, formats, length, decimal_places):
        super().__init__()
        self._keys = keys
        self._required = required
        self._formats = formats
        self._length = length
        self._decimal_places = decimal_places
        
    def __setitem__(self, key, value):
        if not key in self._keys:
            raise KeyError("Adding this key is not allowed. For a list of allowed keys see %s.fields ." % (self.__class__.__name__))
        ed = datatype[self._formats[self._keys.index(key)]]
        if not isinstance(value, ed):
            raise ValueError("The value for the key '{}' needs to be of the datatype {}, not {}".format(key,str(ed), str(type(value))))
        super().__setitem__(key, value)
    
    def valid(self):
        '''Check whether all required fields are filled.'''
        for key,required in zip(self._keys, self._required):
            if required and not key in self:
                return False
        return True
    
    def serialize(self):
        line = []
        for key,format_ in zip(self._keys,self._formats):
            if key in self:
                converter = conv_python2datev[format_]
                line.append( converter(self[key]) )
            elif format_ == 'text':
                line.append('""')
            else:
                line.append('')
        return ';'.join(line)
    
    def parse(self, line, year = None):
        values = line.split(';')
        if not len(values) == len(self._keys):
            raise IOError("Unable to parse line: " + line)
        for key,value,format_ in zip(self._keys, values, self._formats):
            value = value.replace('"','')
            if len(value) == 0:
                continue
            converter = conv_datev2python[format_]
            if format_ == 'date DDMM':
                self[key] = converter(value, year)
            else:
                self[key] = converter(value)
    
    @property
    def allowed_keys(self):
        return self._keys
    
    @property
    def expected_datatypes(self):
        return [datatype[f] for f in self._formats]
    
    @property
    def required_keys(self):
        return [f for f,r in zip(self._keys, self._required) if r]


class DatevEntry2(UserDict):
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
                    raise TypeError("The value for key '{}' needs to be of type float.".format(key))
            elif format_type == 'Datum':
                if not isinstance(value, datetime.date):
                    raise TypeError("The value for key '{}' needs to be of type datetime.date.".format(key))
            elif format_type == 'Datum JJJJMMTT':
                if not isinstance(value, datetime.date):
                    raise TypeError("The value for key '{}' needs to be of type datetime.date.".format(key))
            elif format_type == 'Konto':
                if not isinstance(value, str):
                    raise TypeError("The value for key '{}' needs to be of type str.".format(key))
                if not value.isdigit():
                    raise TypeError("The value for key '{}' needs to be a string of digits.".format(key))
            elif format_type == 'Text':
                if not isinstance(value, str):
                    raise TypeError("The value for key '{}' needs to be of type str.".format(key))
            elif format_type == 'Zahl' and decimal_places == 0:
                if not isinstance(value, int):
                    raise TypeError("The value for key '{}' needs to be of type int.".format(key))
            elif format_type == 'Zahl' and decimal_places > 0:
                if not isinstance(value, float):
                    raise TypeError("The value for key '{}' needs to be of type float.".format(key))
            elif format_type == 'Zeitstempel':
                if not isinstance(value, datetime.datetime):
                    raise TypeError("The value for key '{}' needs to be of type datetime.datetime.".format(key))
            else:
                raise NotImplementedError("Unknown FormatType: {}".format(format_type))
        
        #set value
        super().__setitem__(key, value)
    
    def valid(self):
        '''Check whether all required fields are filled.'''
        for field in self._fields:
            if int(field['Necessary']) == 1:
                if self[field['Label']] is None:
                    raise RuntimeError('Necessary value missing: ' + field['Label'])
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
                raise RuntimeError("The value {} has {} characters, but DATEV file specification allows only {} characters for values at key {}.".format(s, len(s), max_length, key))
        
        return s

    def serialize(self):
        parts = [self.python2datev(field['Label']) for field in self._fields]
        return ';'.join(parts)
    
    def datev2python(self, key, string, year = None):
        '''Parse a astring and save the content as a python datatype in self[key].'''
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
        values = line.split(';')
        if not len(values) == len(self._labels):
            raise IOError("Unable to parse line: " + line)
        for field,value in zip(self._fields, values):
            self.datev2python(field['Label'], value, year)
    
    @property
    def required_keys(self):
        return [field['Label'] for field in self._fields if int(field['Necessary']) == 1]


class Buchung(DatevEntry):
    def __init__(self, umsatz = None, soll_haben = None, konto = None, gegenkonto = None, belegdatum = None):
        '''Buchung'''
        #data format description
        columns = "Umsatz (ohne Soll/Haben-Kz);Soll/Haben-Kennzeichen;WKZ Umsatz;Kurs;Basis-Umsatz;WKZ Basis-Umsatz;Konto;Gegenkonto (ohne BU-Schlüssel);BU-Schlüssel;Belegdatum;Belegfeld 1;Belegfeld 2;Skonto;Buchungstext;Postensperre;Diverse Adressnummer;Geschäftspartnerbank;Sachverhalt;Zinssperre;Beleglink;Beleginfo - Art 1;Beleginfo - Inhalt 1;Beleginfo - Art 2;Beleginfo - Inhalt 2;Beleginfo - Art 3;Beleginfo - Inhalt 3;Beleginfo - Art 4;Beleginfo - Inhalt 4;Beleginfo - Art 5;Beleginfo - Inhalt 5;Beleginfo - Art 6;Beleginfo - Inhalt 6;Beleginfo - Art 7;Beleginfo - Inhalt 7;Beleginfo - Art 8;Beleginfo - Inhalt 8;KOST1 - Kostenstelle;KOST2 - Kostenstelle;Kost-Menge;EU-Land u. UStID;EU-Steuersatz;Abw. Versteuerungsart;Sachverhalt L+L;Funktionsergänzung L+L;BU 49 Hauptfunktionstyp;BU 49 Hauptfunktionsnummer;BU 49 Funktionsergänzung;Zusatzinformation - Art 1;Zusatzinformation- Inhalt 1;Zusatzinformation - Art 2;Zusatzinformation- Inhalt 2;Zusatzinformation - Art 3;Zusatzinformation- Inhalt 3;Zusatzinformation - Art 4;Zusatzinformation- Inhalt 4;Zusatzinformation - Art 5;Zusatzinformation- Inhalt 5;Zusatzinformation - Art 6;Zusatzinformation- Inhalt 6;Zusatzinformation - Art 7;Zusatzinformation- Inhalt 7;Zusatzinformation - Art 8;Zusatzinformation- Inhalt 8;Zusatzinformation - Art 9;Zusatzinformation- Inhalt 9;Zusatzinformation - Art 10;Zusatzinformation- Inhalt 10;Zusatzinformation - Art 11;Zusatzinformation- Inhalt 11;Zusatzinformation - Art 12;Zusatzinformation- Inhalt 12;Zusatzinformation - Art 13;Zusatzinformation- Inhalt 13;Zusatzinformation - Art 14;Zusatzinformation- Inhalt 14;Zusatzinformation - Art 15;Zusatzinformation- Inhalt 15;Zusatzinformation - Art 16;Zusatzinformation- Inhalt 16;Zusatzinformation - Art 17;Zusatzinformation- Inhalt 17;Zusatzinformation - Art 18;Zusatzinformation- Inhalt 18;Zusatzinformation - Art 19;Zusatzinformation- Inhalt 19;Zusatzinformation - Art 20;Zusatzinformation- Inhalt 20;Stück;Gewicht;Zahlweise;Forderungsart;Veranlagungsjahr;Zugeordnete Fälligkeit;Skontotyp;Auftragsnummer;Buchungstyp;USt-Schlüssel (Anzahlungen);EU-Land (Anzahlungen);Sachverhalt L+L (Anzahlungen);EU-Steuersatz (Anzahlungen);Erlöskonto (Anzahlungen);Herkunft-Kz;Buchungs GUID;KOST-Datum;SEPA-Mandatsreferenz;Skontosperre;Gesellschaftername;Beteiligtennummer;Identifikationsnummer;Zeichnernummer;Postensperre bis;Bezeichnung SoBil-Sachverhalt;Kennzeichen SoBil-Buchung;Festschreibung;Leistungsdatum;Datum Zuord. Steuerperiode;Fälligkeit;Generalumkehr (GU);Steuersatz;Land"
        keys = columns.split(';')
        formats = ['float .2','text','text','float .6','float .2','text','account','account','text','date DDMM','text','text','float .2','text','int','text','int','int','int','text'] + 18*['text'] + ['float .4','text','float .2','text','int','int','int','int','int'] + 40*['text'] + ['int','float .2','int','text','int','date DDMMYYYY','int','text','text','int','text','int','float .2','account','text','text','date DDMMYYYY','text','int','text','int','text','text','date DDMMYYYY','text','int','int','date DDMMYYYY','date DDMMYYYY','date DDMMYYYY','text','float .2','text']
        length = [10,1,3,4,10,3,9,9,4,4,36,12,8,60,1,9,3,2,1,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,36,36,12,15,2,1,3,3,1,2,3,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,20,210,8,8,2,10,4,8,1,30,2,2,2,3,2,8,2,36,8,35,1,76,4,11,20,8,30,2,1,8,8,8,1,2,2]
        decimal_places = [2,0,0,6,2,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,4,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0]
        required = [(i in [0,1,6,7,9]) for i in range(121)]
        super().__init__(keys, required, formats, length, decimal_places)
        
        if not umsatz is None:
            self['Umsatz (ohne Soll/Haben-Kz)'] = umsatz
        if not soll_haben is None:
            self['Soll/Haben-Kennzeichen'] = soll_haben
        if not konto is None:
            self['Konto'] = konto
        if not gegenkonto is None:
            self['Gegenkonto (ohne BU-Schlüssel)'] = gegenkonto
        if not belegdatum is None:
            self['Belegdatum'] = belegdatum



class Metadata(DatevEntry):
    '''Metadata of a Datev file.'''
    def __init__(self, entry_type):
        '''
        Parameters
        ----------
        entry_type:  class, one of the classes that inherit from DatevEntry, e.g. Buchung
        '''
        keys = ['DATEV-Format-KZ','Versionsnummer', 'Datenkategorie', 'Formatname', 'Formatversion', 'Erzeugt am', 'Importiert', 'Herkunft', 'Exportiert von', 'Importiert von', 'Berater', 'Mandant', 'Wirtschaftsjahr-Beginn', 'Sachkontennummernlänge', 'Datum von', 'Datum bis', 'Bezeichnung', 'Diktatkürzel', 'Buchungstyp', 'Rechnungslegungszweck', 'Festschreibung', 'Währungskennzeichen', 'reserviert 1', 'Derivatskennzeichen', 'reserviert 2', 'reserviert 3', 'SKR', 'Branchenlösungs-ID', 'reserviert 4', 'reserviert 5', 'Anwendungsinformation']
        formats = ['text', 'int', 'int', 'text', 'int', 'datetime YYYYMMDDhhmmssttt', 'datetime YYYYMMDDhhmmssttt', 'text', 'text', 'text', 'int', 'int', 'date YYYYMMDD', 'int', 'date YYYYMMDD', 'date YYYYMMDD', 'text', 'text', 'int', 'int', 'int', 'text', 'int', 'text', 'int', 'int', 'text', 'int', 'int', 'text', 'text']
        length = [4,3,2,None,3,17,17,2,25,25,7,5,8,1,8,8,30,2,1,2,1,3,None,None,None,None,2,None,None,None,16]
        decimal_places = [0 for key in keys]
        self._required_for_buchungsstapel = [True, True, True, True, True, False, False, False, False, False, True, True, True, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False]
        self._required_for_other_categories = [True, True, True, True, True, False, False, False, False, False, True, True, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False]
        required = self._required_for_buchungsstapel if entry_type == Buchung else self._required_for_other_categories
        super().__init__(keys, required, formats, length, decimal_places)
    
    def valid(self):
        if not super().valid():
            return False
        #Additional health ckecks
        if not self['DATEV-Format-KZ'] in ['DTVF','EXTF']:
            return False
        
    def parse(self, line):
        super().parse(line)
        self._required = self._required_for_buchungsstapel if self['Formatname'] == 'Buchungsstapel' else self._required_for_other_categories



class DatevDataCategory(object):
    '''This is the base class for Datev data categories. Each data category should inherit from this class.''' 
    
    def __init__(self, entry_type):
        
        self._metadata = Metadata(entry_type)
        self._data = []
        self._entry_type = entry_type

        
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
        
        self._metadata = Metadata(self._entry_type)
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
            raise ValueError("The Datev file specification require that the filename has the format EXTF_<arbitrary-name>.csv, e.g. EXTF_Buchungsstapel__<date_time>_<export number>.csv .")
        with open(filename, 'w', encoding = 'ISO-8859-1') as f:
            f.write(self._metadata.serialize() + '\n')
            f.write(self.serialize_data())
    
    @property
    def data(self):
        return self._data
    
    @property
    def metadata(self):
        return self._metadata
    
    def parse_data(self, column_line, entry_lines):
        '''Parse the body of a datev file. 
        
        Parameters
        ----------
        column_line:    string
        entry_lines:    list of strings
        '''
        for line in entry_lines:
            new_entry = self._entry_type()
            new_entry.parse(line, year = self._metadata['Wirtschaftsjahr-Beginn'].year)
            self._data.append(new_entry)
    
    def serialize_data(self):
        '''Serialize the data of the body of a datev file. 
        '''
        lines = []
        #header
        first_entry = self._data[0]
        lines.append(';'.join(first_entry.allowed_keys)) 
        #body
        for entry in self._data:
            lines.append(entry.serialize())
        return '\n'.join(lines)
    
    def export_as_pandas_dataframe(self):
        '''Return data as a pandas DataFrame.'''
        data = []
        for entry in self._data:
            e = []
            for key in entry.allowed_keys:
                if key in entry:
                    e.append(entry[key])
                else:
                    e.append(None)
            data.append(e)
            
        return pd.DataFrame(data, columns = self._data[0].allowed_keys)


class DatevDataCategory2(object):
    '''This is the base class for Datev data categories. Each data category should inherit from this class.''' 
    
    def __init__(self, category_type, version):
        self._category_type = category_type
        self._version = str(version)
        if not category_type in specifications:
            raise ValueError("Unknown category_type: " + category_type)
        if not self._version in specifications[category_type]:
            raise ValueError("Version {} unknown for category {}".format(self._version, category_type))
        self._metadata = DatevEntry2(specifications['Metadaten']['Andere']['Field'])
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
            raise ValueError("The Datev file specification require that the filename has the format EXTF_<arbitrary-name>.csv, e.g. EXTF_Buchungsstapel__<date_time>_<export number>.csv .")
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
        new_entry = DatevEntry2(specifications[self._category_type][self._version]['Field'])
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
            
        return pd.DataFrame(data, columns = self._data[0].keys())


class Buchungsstapel(DatevDataCategory):
    '''Datev Buchungsstapel'''
    #Empfehlung: pro Buchungsperiode eine Textdatei
    #Export-Dateiname: Muss Format EXTF_<beliebig>.csv haben, z.B. EXTF_Buchungsstapel__<Datum_Uhrzeit>_<LfdNr des Exportvorganges>.csv
   
    def __init__(self, filename = None, berater = None, mandant = None, wirtschaftsjahr_beginn = None, sachkontennummernlänge = None, datum_von = None, datum_bis = None ):
        '''If you specify the filename, the data will be loaded from there and the other parameters of this functions are ignored. If you don't specify the filename, a new empty Buchungsstapel will be created using the metadata of the other parameters. 
        
        Parameters
        ----------
        filename:   str
        berater:    int
        mandant:    int
        wirtschaftsjahr_beginn: datetime.date
        sachkontennummernlänge: int
        datum_von:  datetime.date
        datum_bis:  datetime.date
        '''
        super().__init__(Buchung)
        if filename is None:
            if not wirtschaftsjahr_beginn <= datum_von < datum_bis < datetime.date(wirtschaftsjahr_beginn.year+1,wirtschaftsjahr_beginn.month,wirtschaftsjahr_beginn.day):
                raise ValueError("The dates datum_von and datum_bis should be between wirtschaftsjahr_beginn and wirtschaftsjahr_beginn + 1 year.")  
            if not 4 <= sachkontennummernlänge <= 9:
                raise ValueError("The sachkontennummernlänge needs to be between 4 and 9.")
            if not 1 <= mandant <= 99999:
                raise ValueError("The mandant number needs to be between 1 and 99999.")
            if not 1001 <= berater <= 9999999:
                raise ValueError("The berater number needs to be between 1001 and 9999999.")
            self._metadata['DATEV-Format-KZ'] = 'EXTF'
            self._metadata['Versionsnummer'] = 700
            self._metadata['Datenkategorie'] = 21
            self._metadata['Formatname'] = 'Buchungsstapel'
            self._metadata['Formatversion'] = 9
            self._metadata['Berater'] = berater
            self._metadata['Mandant'] = mandant
            self._metadata['Wirtschaftsjahr-Beginn'] = wirtschaftsjahr_beginn
            self._metadata['Datum von'] = datum_von
            self._metadata['Datum bis'] = datum_bis
        else:
            self.load(filename)
        
    def add_entry(self, buchung):
        '''Add entry to the batch. '''
        if len(self._data) == 99999:
            raise RuntimeError("Datev file specification doesn't allow more than 99999 entries.")
        if not isinstance(buchung, Buchung):
            raise ValueError("Buchungsstapel accepts only entries of type Buchung.")
        if not buchung.valid:
            raise RuntimeError("Buchung invalid.")
        
        self._data.append(buchung)
    
    def valid(self):
        '''Check if all file format specifications are satisfied.'''
        if not self._metadata.valid:
            return False
        #all entries need to be valid
        for entry in self._data:
            if not entry.valid:
                return False
        #all entries should be in the same wirtschaftsjahr as metadata['Wirtschaftsjahr-Beginn'] and between metadata['Datum von'] and metadata['Datum bis'] 
        raise NotImplementedError



class Buchungsstapel2(DatevDataCategory2):
    '''Datev Buchungsstapel'''
    #Empfehlung: pro Buchungsperiode eine Textdatei
    #Export-Dateiname: Muss Format EXTF_<beliebig>.csv haben, z.B. EXTF_Buchungsstapel__<Datum_Uhrzeit>_<LfdNr des Exportvorganges>.csv
   
    def __init__(self, filename = None, berater = None, mandant = None, wirtschaftsjahr_beginn = None, sachkontennummernlänge = None, datum_von = None, datum_bis = None, version = 9):
        '''If you specify the filename, the data will be loaded from there and the other parameters of this functions are ignored. If you don't specify the filename, a new empty Buchungsstapel will be created using the metadata of the other parameters. 
        
        Parameters
        ----------
        filename:   str
        berater:    int
        mandant:    int
        wirtschaftsjahr_beginn: datetime.date
        sachkontennummernlänge: int
        datum_von:  datetime.date
        datum_bis:  datetime.date
        version:    int, optional
        '''
        super().__init__("Buchungsstapel", version)
        self._metadata = DatevEntry2(specifications['Metadaten']['Buchungsstapel']['Field'])
        if filename is None:
            if not wirtschaftsjahr_beginn <= datum_von < datum_bis < datetime.date(wirtschaftsjahr_beginn.year+1,wirtschaftsjahr_beginn.month,wirtschaftsjahr_beginn.day):
                raise ValueError("The dates datum_von and datum_bis should be between wirtschaftsjahr_beginn and wirtschaftsjahr_beginn + 1 year.")  
            if not 4 <= sachkontennummernlänge <= 9:
                raise ValueError("The sachkontennummernlänge needs to be between 4 and 9.")
            if not 1 <= mandant <= 99999:
                raise ValueError("The mandant number needs to be between 1 and 99999.")
            if not 1001 <= berater <= 9999999:
                raise ValueError("The berater number needs to be between 1001 and 9999999.")
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
        else:
            self.load(filename)
        
    def add_buchung(self, umsatz = None, soll_haben = None, konto = None, gegenkonto = None, belegdatum = None):
        '''Add Buchung to the batch. '''
        if len(self._data) == 99999:
            raise RuntimeError("Datev file specification doesn't allow more than 99999 entries.")
        entry = self.add_entry()
        entry['Umsatz (ohne Soll/Haben-Kz)'] = umsatz
        entry['Soll/Haben-Kennzeichen'] = soll_haben
        entry['Kontonummer'] = konto
        entry['Gegenkonto (ohne BU-Schlüssel)'] = gegenkonto
        entry['Belegdatum'] = belegdatum
        return entry
    
    def valid(self):
        '''Check if all file format specifications are satisfied.'''
        if not self._metadata.valid:
            return False
        #all entries need to be valid
        for entry in self._data:
            if not entry.valid:
                return False
        #all entries should be in the same wirtschaftsjahr as metadata['Wirtschaftsjahr-Beginn'] and between metadata['Datum von'] and metadata['Datum bis'] 
        raise NotImplementedError




