import unittest
import sql_parser_v2  as parse

class TestSQLParser(unittest.TestCase):
    def setUp(self):
        """Initialisation du parseur avant chaque test."""
        self.parser = parse.SQLParser()

    def test_simple_queries(self):
        """Test des requêtes simples."""
        queries = [
                "show flights from JFK to LAX between 2021-01-01 and 2022-12-31 with departure delay greater than 30",
                "show average departure delay by airline where distance greater than 1000 and arrival delay less than 30",
                "show flights from JFK to LAX between 2023-01-01 and 2023-12-31 with departure delay greater than 30",
                "count number of flights by origin city where distance greater than 500 and arrival delay less than 15",
                "show cancelled flights",
                "show diverted flights",
                "show flights with wheels off time",
                "show flights with taxi in time",
                "show flights from JFK and arrival delay less than 15",
                "show flights to LAX or flights to SFO",
                "show flights with departure delay greater than 30 and arrival delay less than 15",
                "show flights from JFK to LAX with departure delay greater than 30",
                "show average departure delay by airline where distance greater than 1000",
                "show flights from Chicago between 2023-01-01 and 2023-12-31",
                "show mean departure delay by airline",
                "show average distance by origin",
                "show flights with departure delay greater than 30",
                "show flights with arrival delay less than 15",
                "show flights with distance equal to 1000",
                "show flights with air time greater than 120",
                "show flights with departure delay at least 60",
                "show flights with distance at most 500",
                "show flights between 2023-01-01 and 2023-12-31",
                "show flights in year 2023",
                "show flights in month january",
                "show flights before 2023-06-01",
                "show flights after 2023-01-01",
                "show flights during 2023",
                "show flights from last year"
            ]
        for query in queries:
            sql_query = self.parser.parse_to_sql(query)
            print(f"Query: {query}\nGenerated SQL: {sql_query}\n")
            self.assertIsNotNone(sql_query)

    def test_aggregate_queries(self):
        """Test des requêtes d'agrégation."""
        queries = [
            "What is the average departure delay?",
            "Find the total distance traveled by flights",
            "Get the maximum arrival delay",
            "Show the number of flights by airline",
        ]
        for query in queries:
            sql_query = self.parser.parse_to_sql(query)
            print(f"Query: {query}\nGenerated SQL: {sql_query}\n")
            self.assertIsNotNone(sql_query)

    def test_comparison_queries(self):
        """Test des requêtes avec des comparaisons."""
        queries = [
            "Find flights with a departure delay greater than 30 minutes",
            "List flights with an arrival delay less than 15 minutes",
            "Show flights with a distance more than 1000 miles",
        ]
        for query in queries:
            sql_query = self.parser.parse_to_sql(query)
            print(f"Query: {query}\nGenerated SQL: {sql_query}\n")
            self.assertIsNotNone(sql_query)

    def test_temporal_queries(self):
        """Test des requêtes temporelles."""
        queries = [
            "Show flights in 2021",
            "List flights in January",
            "Find flights between 2020 and 2022",
        ]
        for query in queries:
            sql_query = self.parser.parse_to_sql(query)
            print(f"Query: {query}\nGenerated SQL: {sql_query}\n")
            self.assertIsNotNone(sql_query)

    def test_complex_queries(self):
        """Test des requêtes complexes avec plusieurs conditions."""
        queries = [
            "Show flights from New York to Los Angeles that were cancelled",
            "List flights by Delta Airlines in 2021 with a departure delay greater than 20 minutes",
            "Find flights to Chicago in February with an arrival delay less than 10 minutes",
            "Get the total distance traveled by flights in 2020 and 2021",
        ]
        for query in queries:
            sql_query = self.parser.parse_to_sql(query)
            print(f"Query: {query}\nGenerated SQL: {sql_query}\n")
            self.assertIsNotNone(sql_query)


if __name__ == "__main__":
    unittest.main()
