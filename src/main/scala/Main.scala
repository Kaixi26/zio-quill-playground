import io.getquill.jdbczio.Quill
import io.getquill.{jdbczio, _}
import zio.Console._
import zio.{ULayer, ZIO, ZIOAppDefault, ZLayer}

import java.sql.{Date, SQLException}
import javax.sql.DataSource

case class Person(id: Person.Id, name: String, age: Date)

object Person {
  case class Id(value: Int)

  implicit val encodePersonId = MappedEncoding[Id, Int](_.value)
  implicit val decodePersonId = MappedEncoding[Int, Id](Id)
}

case class Phone(id: Option[Phone.Id], number: String, owner: Person.Id)

object Phone {
  case class Id(value: Int)

  implicit val encodePhoneId = MappedEncoding[Id, Int](_.value)
  implicit val decodePhoneId = MappedEncoding[Int, Id](Id)
}

trait DatabaseDataSource extends DataSource

trait Database {

  def initialize: ZIO[DatabaseDataSource, SQLException, Unit]

  def getPeople: ZIO[DatabaseDataSource, SQLException, List[Person]]

  def insertPeople(people: Person*): ZIO[DatabaseDataSource, SQLException, List[Long]]

  def removePerson(id: Person.Id): ZIO[DatabaseDataSource, Throwable, Long]

  def insertPhones(phones: Phone*): ZIO[DatabaseDataSource, SQLException, List[Option[Phone.Id]]]
}

object Database {
  def live(sqliteZioJdbcContext: SqliteZioJdbcContext[SnakeCase], dataSource: ZLayer[Any, Throwable, DataSource]): ZLayer[Any, Throwable, DatabaseDataSource with LocalDatabase] =
    ZLayer.succeed(new LocalDatabase(sqliteZioJdbcContext)) +!+
      dataSource.asInstanceOf[ZLayer[Any, Throwable, DatabaseDataSource]]

  def live(mysqlZioJdbcContext: MysqlZioJdbcContext[SnakeCase], dataSource: ZLayer[Any, Throwable, DataSource]): ZLayer[Any, Throwable, ProductionDatabase with DatabaseDataSource] =
    ZLayer.succeed(new ProductionDatabase(mysqlZioJdbcContext)) +!+
      dataSource.asInstanceOf[ZLayer[Any, Throwable, DatabaseDataSource]]
}

final class ProductionDatabase(ctx: MysqlZioJdbcContext[SnakeCase]) extends Database {

  import ctx._

  def initialize: ZIO[DatabaseDataSource, SQLException, Unit] = for {
    _ <- run(sql"CREATE TABLE IF NOT EXISTS person (id INT PRIMARY KEY, name VARCHAR(255), age DATE)".as[Action[Nothing]])
    _ <- run(sql"CREATE TABLE IF NOT EXISTS phone (id INT PRIMARY KEY AUTO_INCREMENT, number VARCHAR(255) UNIQUE, owner INT, FOREIGN KEY (owner) REFERENCES person(id))".as[Action[Nothing]])
  } yield ()

  override def getPeople: ZIO[DatabaseDataSource, SQLException, List[Person]] = run(query[Person])

  override def insertPeople(people: Person*): ZIO[DatabaseDataSource, SQLException, List[Long]] = run {
    liftQuery(people).foreach { person =>
      query[Person].insertValue(person).onConflictIgnore
    }
  }

  override def removePerson(id: Person.Id): ZIO[DatabaseDataSource, Throwable, Long] = transaction {
    for {
      deletedPhones <- run(query[Phone].filter(phone => phone.owner == lift(id)).delete)
      deletedPeople <- run(query[Person].filter(person => person.id == lift(id)).delete)
    } yield deletedPeople + deletedPhones
  }

  override def insertPhones(phones: Phone*): ZIO[DatabaseDataSource, SQLException, List[Option[Phone.Id]]] = run {
    liftQuery(phones).foreach { phone =>
      query[Phone].insertValue(phone)
        .onConflictIgnore
        .returningGenerated(_.id)
    }
  }
}

final class LocalDatabase(ctx: SqliteZioJdbcContext[SnakeCase]) extends Database {

  import ctx._

  def initialize: ZIO[DatabaseDataSource, SQLException, Unit] = for {
    _ <- run(sql"CREATE TABLE IF NOT EXISTS person (id INT PRIMARY KEY, name VARCHAR(255), age DATE)".as[Action[Nothing]])
    _ <- run(sql"CREATE TABLE IF NOT EXISTS phone (id INTEGER PRIMARY KEY AUTOINCREMENT, number VARCHAR(255) UNIQUE, owner INT, FOREIGN KEY (owner) REFERENCES person(id))".as[Action[Nothing]])
  } yield ()

  override def getPeople: ZIO[DatabaseDataSource, SQLException, List[Person]] = run(query[Person])

  override def insertPeople(people: Person*): ZIO[DatabaseDataSource, SQLException, List[Long]] = run {
    liftQuery(people).foreach { person =>
      query[Person].insertValue(person).onConflictIgnore
    }
  }

  override def removePerson(id: Person.Id): ZIO[DatabaseDataSource, Throwable, Long] = transaction {
    for {
      deletedPhones <- run(query[Phone].filter(phone => phone.owner == lift(id)).delete)
      deletedPeople <- run(query[Person].filter(person => person.id == lift(id)).delete)
    } yield deletedPeople + deletedPhones
  }

  override def insertPhones(phones: Phone*): ZIO[DatabaseDataSource, SQLException, List[Option[Phone.Id]]] = run {
    liftQuery(phones).foreach { phone =>
      query[Phone].insertValue(phone)
        .onConflictIgnore
        .returningGenerated(_.id)
    }
  }
}

object Main extends ZIOAppDefault {
  def main: ZIO[DatabaseDataSource with Database, Throwable, Unit] = {
    val db = ZIO.serviceWithZIO[Database]
    for {
      _ <- db(_.initialize)
      people = List(
        Person(Person.Id(1), "A", Date.valueOf("2008-11-11")),
        Person(Person.Id(2), "B", Date.valueOf("2008-11-12")),
        Person(Person.Id(3), "C", Date.valueOf("2008-11-13")),
        Person(Person.Id(4), "C", Date.valueOf("2008-11-14")),
      )
      updatedRows <- db(_.insertPeople(people: _*))
      _ <- printLine(s"Inserted people, updated rows: $updatedRows")

      phones = List(
        Phone(Some(Phone.Id(0)), "foo", Person.Id(1)),
        Phone(Some(Phone.Id(0)), "bar", Person.Id(2)),
        Phone(None, "baz", Person.Id(4)),
        Phone(None, "bazz", Person.Id(4)),
      )

      insertedPhoneIds <- db(_.insertPhones(phones:_*))
      //insertedPhoneIds <- ZIO.collectAll {
      //  phones.map(phone => db(_.insertPhones(phone)))
      //}
      _ <- printLine(s"Ids for inserted phones: $insertedPhoneIds")


      updatedRows <- db(_.removePerson(Person.Id(4)))
      _ <- printLine(s"Removed id 4, updated rows: $updatedRows")

      retrievedPeople <- db(_.getPeople)
      _ <- printLine(retrievedPeople)

    } yield ()
  }

  override def run = {
    val cfg: Env = Local

    val dblayer = cfg match {
      case Local => Database.live(
        new SqliteZioJdbcContext(SnakeCase),
        Quill.DataSource.fromPrefix("quill-sqlite")
      )
      case Production => Database.live(
        new MysqlZioJdbcContext(SnakeCase),
        Quill.DataSource.fromPrefix("quill-mysql")
      )
    }

    main
      .provide(
        dblayer,
        Quill.DataSource.fromPrefix("UNUSED"),
      )
      .debug("Result: ")
      .exitCode
  }
}

sealed trait Env

object Local extends Env

object Production extends Env
