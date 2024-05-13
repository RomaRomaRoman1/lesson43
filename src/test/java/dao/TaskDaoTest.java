package dao;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import entity.Task;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import javax.sql.DataSource;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TaskDaoTest {
  private TaskDao taskDao;

  @BeforeAll
  public void setUp() throws IOException {
    DataSource dataSource = EmbeddedPostgres.builder()
            .start()
            .getPostgresDatabase();
    taskDao = new TaskDao(dataSource);
    initializeDb(dataSource);
  }

  @BeforeEach
  public void beforeEach() {
    taskDao.deleteAll();
  }

  private void initializeDb(DataSource dataSource) {
    try(InputStream inputStream = this.getClass().getResource("/initial.sql").openStream()) {
      String sql = new String(inputStream.readAllBytes());
      try(
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()
      ) {
        statement.executeUpdate(sql);
      }

    } catch (IOException | SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSaveSetsId() {
    Task task = new Task("test task", false, LocalDateTime.now());
    taskDao.save(task);

    assertThat(task.getId()).isNotNull();
  }

  @Test
  public void testFindAllReturnsAllTasks() {
    Task firstTask = new Task("first task", false, LocalDateTime.now());
    taskDao.save(firstTask);

    Task secondTask = new Task("second task", false, LocalDateTime.now());
    taskDao.save(secondTask);

    assertThat(taskDao.findAll())
      .hasSize(2)
      .extracting("id")
      .contains(firstTask.getId(), secondTask.getId());
  }

  @Test
  public void testDeleteAllDeletesAllRowsInTasks() {
    Task firstTask = new Task("any task", false, LocalDateTime.now());
    Task secondTask = new Task("any second task", false, LocalDateTime.now());
    taskDao.save(firstTask);
    taskDao.save(secondTask);

    int rowsDeleted = taskDao.deleteAll();
    assertThat(rowsDeleted).isEqualTo(2);
    assertThat(taskDao.findAll()).isEmpty();
  }

  @Test
  public void testGetByIdReturnsCorrectTask() {
    Task task = new Task("test task", false, LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS));
    taskDao.save(task);

    assertThat(taskDao.getById(task.getId()))
      .isNotNull()
      .extracting("id", "title", "finished", "createdDate")
      .containsExactly(task.getId(), task.getTitle(), task.getFinished(), task.getCreatedDate());
  }

  @Test
  public void testFindNotFinishedReturnsCorrectTasks()  {
    Task unfinishedTask = new Task("test task", false, LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS));
    taskDao.save(unfinishedTask);

    Task finishedTask = new Task("test task", true, LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS));
    taskDao.save(finishedTask);

    assertThat(taskDao.findAllNotFinished())
      .singleElement()//Этот метод гарантирует, что список содержит только один элемент. Если в списке больше одного элемента или список пустой, тест завершится с ошибкой.
      .extracting("id", "title", "finished", "createdDate")//Этот метод извлекает значения полей id, title, finished и createdDate из элемента списка.
      .containsExactly(unfinishedTask.getId(), unfinishedTask.getTitle(), unfinishedTask.getFinished(), unfinishedTask.getCreatedDate());
    //Метод containsExactly проверяет, что значения извлеченных полей соответствуют ожидаемым значениям unfinishedTask, именно в таком же порядке.
    // Если значения не совпадают или их количество не совпадает, тест завершится с ошибкой.
  }

  @Test
  public void testFindNewestTasksReturnsCorrectTasks() {
    Task firstTask = new Task("first task", false, LocalDateTime.now());
    taskDao.save(firstTask);

    Task secondTask = new Task("second task", false, LocalDateTime.now());
    taskDao.save(secondTask);

    Task thirdTask = new Task("third task", false, LocalDateTime.now());
    taskDao.save(thirdTask);

    assertThat(taskDao.findNewestTasks(2))
      .hasSize(2)
      .extracting("id")
      .containsExactlyInAnyOrder(secondTask.getId(), thirdTask.getId());
  }

  @Test
  public void testFinishSetsCorrectFlagInDb() {
    Task task = new Task("test task", false, LocalDateTime.now());
    taskDao.save(task);

    assertThat(taskDao.finishTask(task).getFinished()).isTrue();
    assertThat(taskDao.getById(task.getId()).getFinished()).isTrue();
  }

  @Test
  public void deleteByIdDeletesOnlyNecessaryData() {
    Task taskToDelete = new Task("first task", false, LocalDateTime.now());
    taskDao.save(taskToDelete);

    Task taskToPreserve = new Task("second task", false, LocalDateTime.now());
    taskDao.save(taskToPreserve);

    taskDao.deleteById(taskToDelete.getId());
    assertThat(taskDao.getById(taskToDelete.getId())).isNull();
    assertThat(taskDao.getById(taskToPreserve.getId())).isNotNull();
  }
}
