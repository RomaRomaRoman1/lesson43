package dao;

import entity.Task;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;

public class TaskDao {
  private final DataSource dataSource;

  public TaskDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public Task save(Task task) {
    String sql = "INSERT INTO task (title, finished, created_date) VALUES (?, ?, ?)";//Создание строки SQL запроса для вставки новой записи в таблицу task
    try(//Начало блока try-with-resources для автоматического закрытия ресурсов.
      Connection connection = dataSource.getConnection();//Получение соединения с базой данных из dataSource.
      PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
        //Создание подготовленного запроса с указанием, что нужно вернуть сгенерированные ключи.
      ) {

      statement.setString(1, task.getTitle());//Установка значения для первого параметра (title) в SQL запросе.
      statement.setBoolean(2, task.getFinished());//Установка значения для второго параметра (finished) в SQL запросе.
      statement.setTimestamp(3, java.sql.Timestamp.valueOf(task.getCreatedDate()));
      //Установка значения для третьего параметра (created_date) в SQL запросе, преобразуя LocalDateTime в Timestamp.
      statement.executeUpdate();//Выполнение SQL запроса на вставку новой записи в базу данных.

      try(ResultSet resultSet = statement.getGeneratedKeys()) {//Получение сгенерированных ключей (если они есть) после выполнения SQL запроса.
        if (resultSet.next()) {//Проверка наличия сгенерированных ключей.
          task.setId(resultSet.getInt(1));//Установка идентификатора (id) объекта Task из сгенерированных ключей.
        }
      }

    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);//Обработка исключения SQLException, которое может возникнуть при выполнении SQL запроса
    }

    return task;
  }

  public List<Task> findAll() {//Объявление метода findAll, который возвращает список объектов Task.
    List<Task> tasks = new ArrayList<>();//Создание пустого списка задач.
    String sql = "SELECT task_id, title, finished, created_date FROM task ORDER BY task_id";
    //Создание строки SQL запроса для выборки всех записей из таблицы task с сортировкой по task_id.
    try(Connection connection = dataSource.getConnection();//Получение соединения с базой данных из dataSource.
        Statement statement = connection.createStatement();
        //Создает объект Statement, который используется для выполнения SQL запросов без параметров к базе данных.
        //Этот объект создается из соединения (Connection), полученного ранее через dataSource.getConnection()
        ResultSet resultSet = statement.executeQuery(sql)//Выполняет SQL запрос, переданный в качестве аргумента методу (sql), и возвращает объект ResultSet,
        // который содержит результаты запроса.
        // В данном случае, метод executeQuery() используется для выполнения запроса на выборку данных из базы данных.
        // Результаты будут доступны для обработки в объекте resultSet
        ) {
      while(resultSet.next()) {//Итерация по результатам запроса.
        final Task task = new Task(//Создание нового объекта Task на основе данных из результатов запроса.
          resultSet.getString(2),//Получение значений полей из результатов запроса.
          resultSet.getBoolean(3),//
          resultSet.getTimestamp(4).toLocalDateTime()
        );
        task.setId(resultSet.getInt(1));//Установка идентификатора (id) объекта Task из результатов запроса.
        tasks.add(task);//Добавление созданного объекта Task в список tasks
      }

    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }

    return tasks;
  }

  public int deleteAll() {
    String sql = "DELETE FROM task"; // SQL запрос для удаления всех записей из таблицы task
    try (Connection connection = dataSource.getConnection();
         PreparedStatement statement = connection.prepareStatement(sql)) {
      return statement.executeUpdate(); // Выполнение SQL запроса на удаление всех записей и возврат количества удаленных строк
    } catch (SQLException throwables) {
      throw new RuntimeException(throwables);
    }
  }

  public Task getById(Integer id) {
    Task task =null;
    String sql = "SELECT task_id, title, finished, created_date FROM task WHERE task_id = ?";//sql запрос с одним подставляемым параметром
    try (Connection connection = dataSource.getConnection();//инициализируем подключение к базе данных
         PreparedStatement statement = connection.prepareStatement(sql)) {//создаем запрос к базе данных
      statement.setInt(1, id);//подставляем параметр в sql апрос id
      ResultSet resultSet = statement.executeQuery();//выполняем запрос и получаем результат
      while (resultSet.next()) {//проверяем результат на содержимое
        // Если есть результаты, создаем объект Task
        task = new Task(resultSet.getString(2),
                resultSet.getBoolean(3),
                resultSet.getTimestamp(4).toLocalDateTime()
        );
        task.setId(resultSet.getInt(1));
      }

    } catch (SQLException e) {
        throw new RuntimeException(e);
    }
      return task;
  }

  public List<Task> findAllNotFinished() {
    List<Task> tasks = new ArrayList<>();
    String sql = "SELECT task_id, title, finished, created_date FROM task WHERE finished = false";
    try (Connection connection = dataSource.getConnection();
    PreparedStatement statement = connection.prepareStatement(sql);
    ResultSet resultSet = statement.executeQuery()) {
      while (resultSet.next()) {
        final Task task = new Task(//Создание нового объекта Task на основе данных из результатов запроса.
                resultSet.getString(2),//Получение значений полей из результатов запроса.
                resultSet.getBoolean(3),//
                resultSet.getTimestamp(4).toLocalDateTime()
        );
        task.setId(resultSet.getInt(1));//Установка идентификатора (id) объекта Task из результатов запроса.
        tasks.add(task);//Добавление созданного объекта Task в список tasks
      }
      }
    catch (SQLException e) {
        throw new RuntimeException(e);
    }
    return tasks;
  }

  public List<Task> findNewestTasks(Integer numberOfNewestTasks) {
    List <Task> tasks = new ArrayList<>();

    String sql = "SELECT task_id, title, finished, created_date FROM task ORDER BY created_date DESC LIMIT ?";
    try(Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql);
    ) {
      statement.setInt(1, numberOfNewestTasks);// Устанавливаем количество последних задач, которые нужно выбрать
      ResultSet resultSet = statement.executeQuery();// Сохранения выполненного запроса в resultSet
      while (resultSet.next()) {
      Task task = new Task(//Создание нового объекта Task на основе данных из результатов запроса.
              resultSet.getString(2),//Получение значений полей из результатов запроса.
              resultSet.getBoolean(3),//
              resultSet.getTimestamp(4).toLocalDateTime()
      );
      task.setId(resultSet.getInt(1));//Установка идентификатора (id) объекта Task из результатов запроса.
      tasks.add(task);//Добавление созданного объекта Task в список tasks
    }
    } catch (SQLException e) {
        throw new RuntimeException(e);
    }
      return tasks;
  }

  public Task finishTask(Task task) {
    String updateSql = "UPDATE task SET finished = true WHERE task_id = ?";
    String selectSql = "SELECT title, finished, created_date FROM task WHERE task_id = ?";
    try (Connection connection = dataSource.getConnection();
         PreparedStatement updateStatement = connection.prepareStatement(updateSql))
    {
      updateStatement.setInt(1, task.getId());
      updateStatement.executeUpdate(); // Выполнение запроса на обновление

      // Теперь, чтобы получить обновленную задачу, выполним SELECT
      try (PreparedStatement selectStatement = connection.prepareStatement(selectSql)) {
        selectStatement.setInt(1, task.getId());
        ResultSet resultSet = selectStatement.executeQuery();
        if (resultSet.next()) {
          // Создание объекта Task на основе результатов запроса
          Task finishedTask = new Task(
                  resultSet.getString("title"),
                  resultSet.getBoolean("finished"),
                  resultSet.getTimestamp("created_date").toLocalDateTime()
          );
          finishedTask.setId(task.getId()); // Установка идентификатора
          return finishedTask;
        } else {
          throw new RuntimeException("Task not found after finishing.");
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void deleteById(Integer id) {
    String sql = "DELETE FROM task WHERE task_id = ?"; // SQL запрос для удаления записи по id
    try(Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql);
            ) {
      statement.setInt(1, id);
      statement.executeUpdate();
    } catch (SQLException e) {
        throw new RuntimeException(e);
    }
  }
}
