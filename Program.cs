using System.Diagnostics;
using MySqlConnector;

namespace PTMK;

public class Program 
{
    static public readonly string ConnectionString = BuildConnectionString();
    static void Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine("Необходимо указать режим работы приложения.");
            return;
        }
 
        if (!int.TryParse(args[0], out var mode))
        {
            Console.WriteLine("Некорректный режим работы.");
            return;
        }

        switch (mode) 
        {
            case 1:
                CreateEmployeeTable(ConnectionString);
                break;
            case 2:
                if (args.Length != 4)
                {
                    Console.WriteLine("Необходимо ввести ФИО, дату рождения и пол.");
                    return;
                }
                AddEmployee(ConnectionString, args[1], DateTime.Parse(args[2]), args[3]);
                break;
            case 3:
                ShowAllEmployees(ConnectionString);
                break;
            case 4:
                FillRandomEmployees(ConnectionString, 1000000);
                FillSpecificEmployees(ConnectionString, 100, "F");
                break;
            case 5:
                SelectEmployeesByCriteria(ConnectionString, "F", "Male");
                break;
            case 6:
                OptimizeDatabase(ConnectionString);
                break;
            default:
                Console.WriteLine("Неизвестный режим работы.");
                break;
        }
    }

    public static string BuildConnectionString()
    {
        var builder = new MySqlConnectionStringBuilder
        {
            Database = "pmtk",
            UserID = "root",
            Password = "",
            Server = "127.0.0.1",
            Port = 3307,
            Pooling = true,
            MinimumPoolSize = 0,
            MaximumPoolSize = 640
        };

        try
        {
            using (var connection = new MySqlConnection(builder.ConnectionString))
            {
                connection.Open();
                Console.WriteLine("DATABASE IS CONNECTED!");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to connect to database: {ex.Message}");
        }

        return builder.ConnectionString;
    }

    // Методы работы с базой данных...
    // (CreateEmployeeTable, AddEmployee, ShowAllEmployees и т.д.)

    static void CreateEmployeeTable(string connectionString)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            try
            {
                connection.Open();
                string query = @"
                CREATE TABLE IF NOT EXISTS Employees (
                    ID INT PRIMARY KEY AUTO_INCREMENT,
                    FullName VARCHAR(255),
                    BirthDate DATE,
                    Gender ENUM('Male', 'Female')
                );";

                MySqlCommand command = new MySqlCommand(query, connection);
                command.ExecuteNonQuery();

                Console.WriteLine("Таблица сотрудников успешно создана.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка: {ex.Message}");
            }
        }
    }

    static void AddEmployee(string connectionString, string fullName, DateTime birthDate, string gender)
    {
        Employee employee = new Employee(fullName, birthDate, gender);

        int age = employee.CalculateAge();
        Console.WriteLine($"Возраст сотрудника {employee.FullName}: {age} лет.");

        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            connection.Open();
            employee.SaveToDatabase(new MySQLDatabaseService(connection));
        }
    }

    static void ShowAllEmployees(string connectionString)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            connection.Open();

            string query = "SELECT FullName, BirthDate, Gender FROM Employees ORDER BY FullName;";
            MySqlCommand command = new MySqlCommand(query, connection);
            using (MySqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    string fullName = reader.GetString("FullName");
                    DateTime birthDate = reader.GetDateTime("BirthDate");
                    string gender = reader.GetString("Gender");

                    Employee employee = new Employee(fullName, birthDate, gender);
                    int age = employee.CalculateAge();

                    Console.WriteLine($"{fullName}, {birthDate:yyyy-MM-dd}, {gender}, {age} лет.");
                }
            }
        }
    }

    static void FillRandomEmployees(string connectionString, int count)
    {
        string[] firstNames = { "John", "Michael", "David", "Paul", "Mark" };
        string[] lastNames = { "Smith", "Johnson", "Williams", "Brown", "Jones" };
        string[] genders = { "Male", "Female" };

        List<Employee> employees = new List<Employee>();

        Random random = new Random();

        for (int i = 0; i < count; i++)
        {
            string fullName = $"{lastNames[random.Next(lastNames.Length)]} {firstNames[random.Next(firstNames.Length)]}";
            DateTime birthDate = new DateTime(random.Next(1950, 2005), random.Next(1, 12), random.Next(1, 28));
            string gender = genders[random.Next(genders.Length)];

            employees.Add(new Employee(fullName, birthDate, gender));
        }

        BatchInsertEmployees(connectionString, employees);
    }

    static void FillSpecificEmployees(string connectionString, int count, string startsWith)
    {
        List<Employee> employees = new List<Employee>();

        Random random = new Random();

        for (int i = 0; i < count; i++)
        {
            string fullName = $"F{random.Next(1000, 9999)} John";
            DateTime birthDate = new DateTime(random.Next(1950, 2005), random.Next(1, 12), random.Next(1, 28));
            employees.Add(new Employee(fullName, birthDate, "Male"));
        }

        BatchInsertEmployees(connectionString, employees);
    }

    static void BatchInsertEmployees(string connectionString, List<Employee> employees)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            connection.Open();
            foreach (var employee in employees)
            {
                employee.SaveToDatabase(new MySQLDatabaseService(connection));
            }
        }
    }

    static void SelectEmployeesByCriteria(string connectionString, string startsWith, string gender)
    {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            connection.Open();

            string query = $"SELECT FullName, BirthDate, Gender FROM Employees WHERE FullName LIKE '{startsWith}%' AND Gender = '{gender}';";
            MySqlCommand command = new MySqlCommand(query, connection);
            using (MySqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    string fullName = reader.GetString("FullName");
                    DateTime birthDate = reader.GetDateTime("BirthDate");
                    gender = reader.GetString("Gender");

                    Employee employee = new Employee(fullName, birthDate, gender);
                    int age = employee.CalculateAge();

                    Console.WriteLine($"{fullName}, {birthDate:yyyy-MM-dd}, {gender}, {age} лет.");
                }
            }
        }

        stopwatch.Stop();
        Console.WriteLine($"Время выполнения запроса: {stopwatch.ElapsedMilliseconds} ms");
    }

    static void OptimizeDatabase(string connectionString)
    {
        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            connection.Open();
            string query = "CREATE INDEX idx_fullname_gender ON Employees (FullName, Gender);";
            MySqlCommand command = new MySqlCommand(query, connection);
            command.ExecuteNonQuery();
            Console.WriteLine("Индексы созданы для ускорения выборки.");
        } 
    }
}

public class Employee
{
    public string FullName { get; set; }
    public DateTime BirthDate { get; set; }
    public string Gender { get; set; }

    public Employee(string fullName, DateTime birthDate, string gender)
    {
        FullName = fullName;
        BirthDate = birthDate;
        Gender = gender;
    }

    public int CalculateAge()
    {
        var today = DateTime.Today;
        var age = today.Year - BirthDate.Year;
        if (BirthDate.Date > today.AddYears(-age)) age--;
        return age;
    }

    public void SaveToDatabase(IDatabaseService databaseService)
    {
        string query = $@"
        INSERT INTO Employees (FullName, BirthDate, Gender)
        VALUES ('{FullName}', '{BirthDate:yyyy-MM-dd}', '{Gender}');";

        databaseService.ExecuteQuery(query);
    }
}

public interface IDatabaseService
{
    void ExecuteQuery(string query);
}

public class MySQLDatabaseService : IDatabaseService
{
    private readonly MySqlConnection _connection;

    public MySQLDatabaseService(MySqlConnection connection)
    {
        _connection = connection;
    }

    public void ExecuteQuery(string query) 
    {
        MySqlCommand command = new MySqlCommand(query, _connection);
        command.ExecuteNonQuery();
    }
}