from typing import Optional, Dict, List, Any, Tuple
import re
from datetime import datetime
import logging
from enum import Enum
from functools import lru_cache

class QueryType(Enum):
    SELECT = "SELECT DISTINCT "
    AGGREGATE = "AGGREGATE"
    COMPARISON = "COMPARISON"
    TEMPORAL = "TEMPORAL"

class SQLParser:
    def __init__(self):
        self.setup_logging()
        self.initialize_mappings()
        self.compiled_patterns = self._compile_regex_patterns()
        
    def _compile_regex_patterns(self) -> Dict[str, re.Pattern]:
        """Pre-compile regex patterns for better performance"""
        return {
            'whitespace': re.compile(r'\s+'),
            'date': re.compile(r'\d{4}-\d{2}-\d{2}'),
            'year': re.compile(r'\b\d{4}\b'),
            'numbers': re.compile(r'\d+(?:\.\d+)?'),
            
            'conditions': re.compile(r'\b(and|or)\b(?!\s*\d)', flags=re.IGNORECASE),
            'exact_matches': {
                'from': re.compile(r'from\s+(\w+)', re.IGNORECASE),
                'to': re.compile(r'to\s+(\w+)', re.IGNORECASE),
                'airline': re.compile(r'airline\s+(\w+)', re.IGNORECASE),
                'origin_city': re.compile(r'origin city\s+([A-Za-z\s]+)', re.IGNORECASE),
                'dest_city': re.compile(r'destination city\s+([A-Za-z\s]+)', re.IGNORECASE)
            }
        }

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def initialize_mappings(self):
        """Initialize all mapping dictionaries for parsing"""
        self.aggregate_functions = {
            "average": "AVG",
            "mean": "AVG",
            "sum": "SUM",
            "total": "SUM",
            "count number of": "COUNT",
            "count": "COUNT",
            "number of": "COUNT",
            "a maximum of": "MAX",
            "maximum of": "MAX",
            "a maximum": "MAX",
            "maximum": "MAX",
            "max": "MAX",
            "highest": "MAX",
            "a minimum of": "MIN",
            "minimum of": "MIN",
            "a minimum": "MIN",
            "minimum": "MIN",
            "min": "MIN",
            "lowest": "MIN"
        }

        self.comparison_operators = {
            "equal to": "=",
            "equals": "=",
            "greater than": ">",
            "more than": ">",
            "less than": "<",
            "fewer than": "<",
            "at least": ">=",
            "at most": "<=",
            "not equal": "!="
        }

        self.temporal_keywords = {
            "year": "Year",
            "month": "Month",
            "day": "DayofMonth",
            "between": "BETWEEN",
            "before": "<",
            "after": ">",
            "during": "BETWEEN"
        }

        self.column_mappings = {
            "flight number": "Flight_Number_Operating_Airline",
            "marketing flight": "Flight_Number_Marketing_Airline",
            "airline": "Airline",
            "city codes" : "Origin, OriginCityName",
            "airline company": "Airline",
            "airline name": "Airline",
            "flights": "FlightDate,Airline,OriginCityName,DestCityName, Cancelled, Diverted, DepDelayMinutes, ArrDelayMinutes",
            "marketing airline": "Marketing_Airline_Network",
            "origin": "Origin",
            "origin city": "OriginCityName",
            "origin state": "OriginStateName",
            "destination": "Dest",
            "destination city": "DestCityName",
            "destination state": "DestStateName",
            "departure delay": "DepDelay",
            "arrival delay": "ArrDelay",
            "cancelled": "Cancelled",
            "diverted": "Diverted",
            "date": "FlightDate",
            "distance": "Distance",
            "air time": "AirTime",
            "elapsed time": "ActualElapsedTime",
            "scheduled time": "CRSElapsedTime",
            "departure time": "DepTime",
            "arrival time": "ArrTime",
            "taxi out": "TaxiOut",
            "taxi in": "TaxiIn",
            "wheels off": "WheelsOff",
            "wheels on": "WheelsOn"
        }

        self.table_name = "flights"


    @lru_cache(maxsize=128)
    def clean_input(self, text: str) -> str:
        """Clean and normalize input text with caching"""
        text = text.strip()
        return self.compiled_patterns['whitespace'].sub(' ', text)

    @lru_cache(maxsize=128)
    def _check_query_type(self, query: str) -> Tuple[bool, bool, bool]:
        """Cached helper for query type checking"""
        has_aggregate = any(func in query for func in self.aggregate_functions)
        has_comparison = any(op in query for op in self.comparison_operators)
        has_temporal = any(temp in query for temp in self.temporal_keywords)
        return has_aggregate, has_comparison, has_temporal

    def identify_query_type(self, query: str) -> QueryType:
        """Optimized query type identification"""
        has_aggregate, has_comparison, has_temporal = self._check_query_type(query)
        if has_aggregate:
            return QueryType.AGGREGATE
        elif has_comparison:
            return QueryType.COMPARISON
        elif has_temporal:
            return QueryType.TEMPORAL
        return QueryType.SELECT

    def parse_temporal_condition(self, text: str) -> str:
        """Optimized temporal condition parsing"""
            # Handle date ranges (FlightDate BETWEEN '2021-01-01' AND '2022-12-31')
        dates = self.compiled_patterns['date'].findall(text)
       #print(dates)
        if "between" in text and len(dates) == 2:
                return f"FlightDate BETWEEN '{dates[0]}' AND '{dates[1]}'"
        elif (len(dates) == 1) and ("before" in text or "after" in text or "in" in text):
                 return f"FlightDate {'<' if 'before' in text else '>' if  "after" in text else '='  } {dates[0]}"  # Single year condition like "Year > 2021" or "Year = 2021"  # Single year condition like "Year < 2021" :

        # Handle specific years (e.g. Year = 2021 or Year = 2022)
        else :
            years = self.compiled_patterns['year'].findall(text)
           #print(years)
            if years:
                if len(years) == 2 and "between" in text:
                    return f"Year BETWEEN {years[0]} AND {years[1]}"  # Combine into a range like "Year BETWEEN 2021 AND 2022"
                elif (len(years) == 1) and ("before" in text or "after" in text or "in" in text):
                    return f"Year {'<' if 'before' in text else '>' if  "after" in text else '='  } {years[0]}"  # Single year condition like "Year > 2021" or "Year = 2021"  # Single year condition like "Year < 2021" :
                    #return f"Year = {years[0]}"  # Single year condition like "Year = 2021"

            # Handle relative dates (e.g. last year)
            if "last year" in text:
                current_year = datetime.now().year
                return f"Year = {current_year - 1}"

            # Handle month conditions (e.g. "January", "February")
        months = {"january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
                "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12}
        for month_name, month_num in months.items():
            if month_name in text.lower():
                return f"Month = {month_num}"

        return ""



    @lru_cache(maxsize=64)
    def parse_aggregate_condition(self, text: str) -> tuple:
        """
        Parse the aggregate condition from the input text.
        - Handles multi-word column names.
        - Identifies the aggregation function and maps it to the correct column.
        Returns: A tuple of the SQL aggregate expression and its alias.
        """
        aggregate_column = None
        agg_func = None

        # Step 1: Identify the aggregation function
        for agg_key, agg_function in self.aggregate_functions.items():
            if agg_key in text:
               #print("aggkey", agg_key)
                agg_func = agg_function
                break

        if not agg_func:
            return "", ""  # No valid aggregation function found

        # Step 2: Extract the potential target column (multi-word support)
        words = text.split()
       #print(words)
        try:
            agg_index = words.index(agg_key.split()[-1])
           #print(agg_index)
           #print('aggindex', agg_index)
            # Collect words after the aggregation key
            possible_column = " ".join(words[agg_index + 1:]).strip()
           #print('poddiirl',possible_column)

            # Match the possible column to the mapping (longest match first)
            for col_key, col_name in sorted(self.column_mappings.items(), key=lambda x: -len(x[0])):
                if possible_column.startswith(col_key):
                    aggregate_column = col_name
                    break
        except (ValueError, IndexError):
            # Handle cases where the key is at the end of the text or column not found
           #print('icic')
            pass

       #print(f"{agg_func}({aggregate_column})", f"agg_{aggregate_column}")
        # Ensure both aggregation function and column are identified
        if agg_func and aggregate_column:
            return f"{agg_func}({aggregate_column.split(',')[0]})", "aggregat"
        
        # Fallback for incomplete parsing
        return "", ""



    @lru_cache(maxsize=64)
    def parse_comparison_condition(self, text: str) -> str:
        """Optimized comparison condition parsing with precise number matching"""
        conditions = []
        matches = []

        # Extraire tous les nombres et leur position dans le texte
        for match in self.compiled_patterns['numbers'].finditer(text):
            matches.append({'value': match.group(), 'start': match.start(), 'end': match.end()})

        if not matches:
            return ""

        # Pour chaque opérateur, chercher la correspondance avec une colonne
        for op_key, op_symbol in self.comparison_operators.items():
            if op_key in text:
               #print("opkey:", op_key)
                for col_key, col_name in self.column_mappings.items():
                    if col_key in text:
                        # Trouver le nombre le plus proche de la colonne et de l'opérateur
                        for match in matches:
                            # Vérifier si le nombre est "proche" de la colonne et de l'opérateur
                            col_pos = text.find(col_key)
                            op_pos = text.find(op_key)
                            if col_pos < op_pos< match['start']:
                                # Ajouter la condition avec le nombre correspondant
                                if col_name == "DepDelay" and "departure delay" in text:
                                    conditions.append(f"{col_name} {op_symbol} {match['value']}")
                                elif col_name == "ArrDelay" and "arrival delay" in text:
                                    conditions.append(f"{col_name} {op_symbol} {match['value']}")
                                elif col_name == "Distance"  and ("distance" in text or "Distance" in text):
                                    conditions.append(f"{col_name} {op_symbol} {match['value']}")
                                elif col_name == "AirTime" and "air time" in text:
                                    conditions.append(f"{col_name} {op_symbol} {match['value']}")
                                elif col_name == "ActualElapsedTime" and "elapsed time" in text:
                                    conditions.append(f"{col_name} {op_symbol} {match['value']}")
                                elif col_name == "CRSElapsedTime" and "scheduled time" in text:
                                    conditions.append(f"{col_name} {op_symbol} {match['value']}")
                                elif col_name == "DepTime" and "departure time" in text:
                                    conditions.append(f"{col_name} {op_symbol} {match['value']}")
                                # Retirer ce nombre pour éviter une double utilisation
                                matches.remove(match)
                                break

        return " AND ".join(conditions)



    def parse_exact_matches(self, text: str) -> List[str]:
        """Optimized exact match parsing"""
        conditions = []
        patterns_to_columns = {
            'from': 'Origin', ###
            'to': 'Dest',
            'airline': 'Airline',
            'origin_city': 'OriginCityName',
            'dest_city': 'DestCityName'
        }
        
        for pattern_key, column in patterns_to_columns.items():
            if match := self.compiled_patterns['exact_matches'][pattern_key].search(text):
                value = match.group(1).strip()
                if column.endswith('Name') or column in ['Origin', 'Dest', 'Airline']:
                    conditions.append(f"{column} = '{value}'")
                else:
                    conditions.append(f"{column} = {value}")
        
        return conditions




    def parse_where_conditions(self, text: str) -> List[str]:
        """Optimized WHERE condition parsing, including clauses with 'with' and 'where'."""
        conditions = []
        
       #print("texte:", text)
        
        # Handle splitting on both 'with' and 'where'
        parts = text.replace("where", "with").split("with")  # Normalize 'where' as 'with'
       #print('parts', parts)
        
        # Parse main condition (before the first 'with' or 'where')
        main_condition = parts[0].strip()
        if temporal_condition := self.parse_temporal_condition(main_condition):
            conditions.append(temporal_condition)
        if comparison_condition := self.parse_comparison_condition(main_condition):
            conditions.append(comparison_condition)
        conditions.extend(self.parse_exact_matches(main_condition))
        
        # Parse additional conditions introduced by 'with' or 'where'
        if len(parts) > 1:
            for additional_condition in parts[1:]:  # Iterate through all remaining parts
                additional_condition = additional_condition.strip()
                if comparison_condition := self.parse_comparison_condition(additional_condition):
                    conditions.append(comparison_condition)
                   #print('comp_cond', comparison_condition)
                if temporal_condition := self.parse_temporal_condition(additional_condition):
                    conditions.append(temporal_condition)
                   #print('temp_cond', temporal_condition)
                conditions.extend(self.parse_exact_matches(additional_condition))
        
        # Handle specific keywords
        if "diverted" in text.lower():
            conditions.append("Diverted = true")
        if "cancelled" in text.lower():
            conditions.append("Cancelled = true")
        return conditions



    def parse_multiple_conditions(self, text: str) -> List[str]:
        """Optimized multiple condition parsing"""
        conditions = []
        parts = self.compiled_patterns['conditions'].split(text)
        #print('parts',parts)
        
        if len(parts) > 1:
            for i in range(0, len(parts)-1, 2):
                condition = parts[i]
                operator = parts[i+1].upper()
                next_condition = parts[i+2]
                
                first_cond = self.parse_where_conditions(condition)
               #print("condition",condition)
                second_cond = self.parse_where_conditions(next_condition)
               #print("next coindition",next_condition)
                
                if first_cond and second_cond:
                    conditions.append(f"({first_cond[0]} {operator} {second_cond[0]})")
        
        return conditions

    def build_select_clause(self, text: str) -> str:
        """Optimized SELECT clause building with ordered columns"""
        selected_columns = []  # Using list to maintain insertion order
        
        for col_key, col_name in self.column_mappings.items():
            if col_key in text and col_name not in selected_columns:
                for i in col_name.split(','):
                    if i not in selected_columns:
                        selected_columns.append(i.strip())

        #print(selected_columns)
        return "SELECT DISTINCT " + (", ".join(selected_columns) if selected_columns else "*")


    def build_where_clause(self, text: str) -> str:
        """Optimized WHERE clause building"""
        all_conditions = []
        parts = self.compiled_patterns['conditions'].split(text)
        #print('parts',parts)
        
        if len(parts) == 1:
        # Get basic conditions
            all_conditions.extend(self.parse_where_conditions(text))
        
        else :
        # Get complex conditions (with AND/OR)
           all_conditions.extend(self.parse_multiple_conditions(text))
        
        if all_conditions:
            return "WHERE " + " AND ".join(all_conditions)
        return ""

    def parse_to_sql(self, natural_query: str) -> Optional[str]:
        """Optimized SQL query generation"""
        try:
            cleaned_query = self.clean_input(natural_query)
            query_type = self.identify_query_type(cleaned_query)
            
            # Initialize query components
            query_parts = []
            select_clause = self.build_select_clause(cleaned_query)
            where_clause = self.build_where_clause(cleaned_query)
            
            # Handle aggregation
            if query_type == QueryType.AGGREGATE:
                agg_function, agg_alias = self.parse_aggregate_condition(cleaned_query)
                if agg_function:
                    Select_statement=  "SELECT"
                    select_clause = f" {agg_function} "
                    group_by = []
                    for col_key, col_name in self.column_mappings.items():
                        if f"by {col_key}" in cleaned_query:
                            group_by.append(col_name)
                            select_clause = f"{col_name}," +select_clause 
                   #print("slect clause", select_clause)
                    select_clause = Select_statement +' '+ select_clause + f" AS {agg_alias}"
                    if group_by:
                        query_parts.extend([select_clause, 
                                         f"FROM {self.table_name}",
                                         where_clause,
                                         "GROUP BY " + ", ".join(group_by)])
                    else:
                        query_parts.extend([select_clause, 
                                         f"FROM {self.table_name}",
                                         where_clause])
            else:
                query_parts.extend([select_clause, 
                                  f"FROM {self.table_name}",
                                  where_clause])

            sql_query = " ".join(filter(None, query_parts))
            #self.logger.info(f"Generated SQL query: {sql_query}")
            return sql_query

        except Exception as e:
            self.logger.error(f"Error generating SQL query: {str(e)}")
            return None
