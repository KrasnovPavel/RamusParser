using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Xml;
using System.IO.Compression;

namespace RamusParser
{
    public static class RamusParser
    {
        private static SQLiteConnection _database;
        private static Dictionary<string, List<Element>> _data = new Dictionary<string, List<Element>>();

        public class Element
        {
            public readonly string Name;
            public readonly string Type;
            public readonly string Start;
            public readonly string End;

            public Element(string name, string type, string start, string end)
            {
                Name = name;
                Type = type;
                End = end;
                Start = start;
            }
        }

        public static void ReadRamusFile(string inputFileName, string outputFileName)
        {
            if (Directory.Exists("./tmp"))
            {
                Directory.Delete("./tmp", true);
            }
            ZipFile.ExtractToDirectory(inputFileName, "./tmp/");

            List<List<object>> textsData     = ReadXmlFile("./tmp/data/Core/attribute_texts.xml");
            List<List<object>> arrowsData    = ReadXmlFile("./tmp/data/IDEF0/attribute_sector_borders.xml");
            List<List<object>> otherData     = ReadXmlFile("./tmp/data/Core/attribute_other_elements.xml");
            List<List<object>> hierarchyData = ReadXmlFile("./tmp/data/Core/attribute_hierarchicals.xml");

            try
            {
                CreateSQLiteDatabase();
                FillTable(textsData,     "texts");
                FillTable(arrowsData,    "arrows");
                FillTable(otherData,     "other_elements");
                FillTable(hierarchyData, "hierarchy");

                ReadFunctionsHierarchy();
                ReadFunctions();
                ReadArrows();
                JoinArrows();
                
                WriteCSV(outputFileName);
            }
            catch (SQLiteException e)
            {
                Console.WriteLine("SQLite error: " + e.Message);
                throw;
            }
        }

        private static void JoinArrows()
        {
            foreach (string key in _data.Keys)
            {
                for (int i = 0; i < _data[key].Count; i++)
                {
                    for (int j = _data[key].Count - 1; j >= i + 1; j--)
                    {
                        if (_data[key][i].Name != _data[key][j].Name) continue;
                        
                        _data[key][i] = new Element(_data[key][i].Name, 
                                                    JoinStrings(_data[key][i].Type,  _data[key][j].Type), 
                                                    JoinStrings(_data[key][i].Start, _data[key][j].Start), 
                                                    JoinStrings(_data[key][i].End,   _data[key][j].End));
                        _data[key].RemoveAt(j);
                    }
                }
            }
        }

        private static string JoinStrings(string first, string second)
        {
            HashSet<string> set = new HashSet<string>();
            foreach (string s in first.Split('/'))
            {
                set.Add(s);
            }

            foreach (string s in second.Split('/'))
            {
                set.Add(s);
            }

            return string.Join("/", set);
        }
        
        private static void WriteCSV(string filename)
        {
            using (StreamWriter file = File.CreateText(filename))
            {
                foreach (string key in _data.Keys)
                {
                    file.WriteLine("Диаграмма:" + key + ";;;;");
                    file.WriteLine("Название компонента;Тип компонента;Начало;Окончание;");
                    foreach (Element element in _data[key])
                    {
                        file.WriteLine("{0};{1};{2};{3};", element.Name, element.Type, element.Start, element.End);
                    }
                }
            }
        }

        private static void ReadFunctionsHierarchy()
        {
            SQLiteCommand command = new SQLiteCommand
            {
                Connection = _database,
                CommandText = @"SELECT DISTINCT parent FROM function_hierarchy;"
            };

            SQLiteDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                _data[reader.GetValue(0).ToString()] = new List<Element>();
            }
            reader.Close();
        }

        private static void ReadFunctions()
        {
            SQLiteCommand command = new SQLiteCommand
            {
                Connection = _database,
                CommandText = @"SELECT element FROM function_hierarchy WHERE parent = ?;"
            };
            command.Parameters.Add(command.CreateParameter());

            foreach (string key in _data.Keys)
            {
                command.Parameters[0].Value = key;
                SQLiteDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    _data[key].Add(new Element(reader.GetValue(0).ToString(), "Функциональный блок", "", ""));
                }
                reader.Close();
            }
        }

        private static void ReadArrows()
        {
            SQLiteCommand command = new SQLiteCommand
            {
                Connection = _database,
                CommandText = @"SELECT id, text, function, type, enter_type, parent
                                FROM all_arrows
                                WHERE id in (SELECT id FROM all_arrows GROUP BY id HAVING COUNT(id) = 2)
                                ORDER BY id, enter_type;"
            };
            SQLiteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                string name = reader.GetValue(1).ToString();
                string type = reader.GetValue(3).ToString();
                string end = reader.GetValue(2).ToString();
                string parent = reader.GetValue(5).ToString();

                reader.Read();

                if (type == "")
                {
                    type = reader.GetValue(3).ToString();
                }
                else if (type != reader.GetValue(3).ToString() && reader.GetValue(3).ToString() != "")
                {
                    type += "/" + reader.GetValue(3);
                }

                string start = reader.GetValue(2).ToString();
                if (parent.Length == 0)
                {
                    parent = reader.GetValue(5).ToString();
                }
                
                _data[parent].Add(new Element(name.Trim(), type.Trim(), start.Trim(), end.Trim()));
            }
            reader.Close();
        }

        private static void CreateSQLiteDatabase()
        {
            _database = new SQLiteConnection("Data Source=:memory:;Version=3;MultipleActiveResultSets=true");
            _database.Open();
            ConstructSQLiteTables();
        }

        private static void ConstructSQLiteTables()
        {
            SQLiteCommand sqlCommand = new SQLiteCommand
            {
                Connection = _database,
                CommandText = @"CREATE TABLE texts(type INT , id INT, text TEXT, value_branch_id INT);
                                INSERT INTO texts VALUES(55, -1, 'border', 0);

                                CREATE TABLE arrows(attribute_id INT, border_type INT, crosspoint INT,
                                                    element_id INT, function INT, function_type INT,
                                                    tunnel_soft INT, value_branch_id INT);

                                CREATE TRIGGER arrow_existence AFTER INSERT ON arrows
                                WHEN 2 <= (SELECT COUNT(arrows.element_id) 
                                            FROM arrows 
                                            WHERE element_id = NEW.element_id AND function = -1)
                                BEGIN 
                                    DELETE FROM arrows WHERE element_id = new.element_id;
                                END;

                                CREATE TABLE other_elements(attribute_id INT, element_id INT,
                                                            other_element INT, value_branch_id INT);

                                CREATE TABLE attributes(id INT, text TEXT);
                                INSERT INTO attributes VALUES(41, 'Из');
                                INSERT INTO attributes VALUES(42, 'В');
                                INSERT INTO attributes VALUES(55, 'Функциональный блок');
                                INSERT INTO attributes VALUES(39, 'Стрелка');
                                INSERT INTO attributes VALUES(-1, 'border');

                                CREATE TABLE types(id INT, text TEXT);
                                INSERT INTO types VALUES(-1, null);
                                INSERT INTO types VALUES(0,  'Выход');
                                INSERT INTO types VALUES(1, 'Механизм');
                                INSERT INTO types VALUES(2, 'Вход');
                                INSERT INTO types VALUES(3, 'Управление');

                                CREATE TABLE hierarchy(attribute_id INT, element_id INT, icon_id INT,
                                                       parent_element_id INT, previous_element_id INT, branch_id INT);

                                CREATE VIEW function_hierarchy AS
                                SELECT hierarchy.element_id AS id, t1.text AS element, COALESCE(t2.text, 'Корень') AS parent
                                FROM hierarchy
                                    LEFT JOIN texts AS t1 ON hierarchy.element_id = t1.id
                                    LEFT JOIN texts AS t2 ON hierarchy.parent_element_id = t2.id
                                WHERE t1.type == 55;

                                CREATE VIEW all_arrows AS
                                SELECT DISTINCT arrows.element_id AS id,
                                                t2.text AS text,
                                                attributes.text AS enter_type,
                                                t1.text AS function,
                                                COALESCE(types1.text, '') || COALESCE(types2.text, '') AS type,
                                                function_hierarchy.parent
                                FROM arrows
                                    LEFT JOIN texts AS t1 ON arrows.function = t1.id
                                    LEFT JOIN attributes ON arrows.attribute_id = attributes.id
                                    LEFT JOIN other_elements ON arrows.element_id = other_elements.element_id
                                    LEFT JOIN texts AS t2 ON other_elements.other_element = t2.id
                                    LEFT JOIN types AS types1 ON arrows.function_type = types1.id
                                    LEFT JOIN types AS types2 ON arrows.border_type = types2.id
                                    LEFT JOIN function_hierarchy ON arrows.function = function_hierarchy.id
                                WHERE t2.type == 39;"
            };
            sqlCommand.ExecuteNonQuery();
        }

        private static void FillTable(List<List<object>> table, string tableName)
        {
            if (table == null || table.Count == 0)
            {
                return;
            }

            int columnsCount = table.First().Count;

            SQLiteCommand command = BuildInsertCommand(tableName, columnsCount);

            foreach (List<object> row in table)
            {
                for (int i = 0; i < columnsCount; i++)
                {
                    command.Parameters[i].Value = row[i];
                }

                command.ExecuteNonQuery();
            }
        }

        private static SQLiteCommand BuildInsertCommand(string tableName, int columnsCount)
        {
            SQLiteCommand command = new SQLiteCommand
            {
                Connection = _database,
                CommandText = "INSERT INTO " + tableName + " VALUES("
            };

            for (int i = 0; i < columnsCount - 1; i++)
            {
                command.CommandText += "?, ";
                command.Parameters.Add(command.CreateParameter());
            }

            command.CommandText += "?);";
            command.Parameters.Add(command.CreateParameter());
            
            return command;
        }
        
        private static List<List<object>> ReadXmlFile(string filename)
        {
            List<List<object>> parsedData = new List<List<object>>();

            XmlDocument doc = new XmlDocument();
            doc.Load(filename);
            if (doc.DocumentElement == null)
            {
                return parsedData;
            }

            XmlNode data = doc.DocumentElement.LastChild;
            foreach (XmlNode row in data.ChildNodes)
            {
                parsedData.Add(new List<object>());
                foreach (XmlNode field in row)
                {
                    if (int.TryParse(field.InnerXml, out int value))
                    {
                        parsedData.Last().Add(value);
                    }
                    else
                    {
                        parsedData.Last().Add(field.InnerXml);
                    }
                }
            }

            return parsedData;
        }
    }
}