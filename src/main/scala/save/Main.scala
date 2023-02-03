package save

import io.getquill.context.qzio.ZioJdbcContext
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.{Action, CamelCase, MappedEncoding, MysqlZioJdbcContext, NamingStrategy, Query, Quoted, SnakeCase, SqliteDialect, SqliteZioJdbcContext}
import io.getquill.jdbczio.Quill
import io.getquill.mirrorContextWithQueryProbing.{performIO, runIO}
import zio.{ZIO, ZIOAppDefault, ZLayer}
import zio.Console._

import java.sql.{Date, SQLException}

case class PersonId(value: Int)

object PersonId {
  implicit val encodePersonId = MappedEncoding[PersonId, Int](_.value)
  implicit val decodePersonId = MappedEncoding[Int, PersonId](PersonId(_))
}

case class Person(id: PersonId, name: String, age: Date)

object Person {
  implicit val encodePerson = MappedEncoding[Person, (PersonId, String, Date)](person => (person.id, person.name, person.age))
  implicit val decodePerson = MappedEncoding[(PersonId, String, Date), Person] { case (id, str, i) => Person(id, str, i) }
}

case class Phone(id: Option[Int], number: String, owner: PersonId)

class DataService(quill: Quill.Sqlite[SnakeCase]) {

  import quill._

  def initialize: ZIO[Any, SQLException, Unit] = for {
    _ <- run(sql"CREATE TABLE IF NOT EXISTS person (id INT PRIMARY KEY, name VARCHAR(255), age DATE)".as[Action[Nothing]])
    people = List(
      Person(PersonId(1), "Joe Bloggs", Date.valueOf("2008-11-11")),
      Person(PersonId(2), "Jim Roggs", Date.valueOf("2008-11-12")),
      Person(PersonId(3), "Vlad Dracul", Date.valueOf("2008-11-11")),
    )
    _ <- ZIO.collectAll {
      people.map(person => run(query[Person].insertValue(lift(person)).onConflictIgnore))
    }

    _ <- run(sql"CREATE TABLE IF NOT EXISTS phone (id INTEGER PRIMARY KEY AUTOINCREMENT, number VARCHAR(255) UNIQUE, owner INT, FOREIGN KEY (owner) REFERENCES person(id))".as[Action[Nothing]])
    //_ <- run(sql"CREATE TABLE IF NOT EXISTS phone (id INT PRIMARY KEY AUTO_INCREMENT, number VARCHAR(255) UNIQUE, owner INT, FOREIGN KEY (owner) REFERENCES person(id))".as[Action[Nothing]])

    phones = List(
      Phone(None, "10", PersonId(1)),
      Phone(None, "11", PersonId(1)),
      Phone(None, "12", PersonId(1)),
      Phone(None, "20", PersonId(2)),
      Phone(None, "30", PersonId(3)),
    )
    _ <- ZIO.collectAll {
      phones.map(phone => run(query[Phone].insertValue(lift(phone)).onConflictIgnore))
    }

  } yield ()

  def getPeople: ZIO[Any, SQLException, List[Person]] = run(query[Person])

  def getPeopleRaw: ZIO[Any, SQLException, List[Person]] = run {
    sql"""SELECT id, name, age FROM person"""
      .as[Query[Person]]
  }

  def getPeoplePhones: ZIO[Any, SQLException, Map[Person, List[Phone]]] = run {
    query[Person]
      .leftJoin(query[Phone])
      .on((person, phone) => person.id == phone.owner)
  }.map { result =>
    result
      .groupBy(_._1)
      .view
      .mapValues(vs => vs.flatMap(_._2))
      .toMap
  }

  def getPeoplePhones_ = run {
    (query[Person].filter(_.name == "woman"))
      .leftJoin(query[Phone])
      .on((person, phone) => person.id == phone.owner)
      .map(result => result._2.map(_.number))
  }
  def getPeoplePhones2 = run {
    query[Person]
      .join(query[Phone])
      .on((person, phone) => person.id == phone.owner)
  }

  def putPerson(person: Person): ZIO[Any, SQLException, Long] = run {
    query[Person]
      .insertValue(lift(person))
  }

  def putPhone(phone: Phone): ZIO[Any, SQLException, Option[Int]] = run {
    query[Phone]
      .insertValue(lift(phone))
      .returningGenerated(_.id)
  }

  def deletePersonById(id: PersonId): ZIO[Any, Throwable, Long] = transaction {
    for {
      deletedPhones <- run(query[Phone].filter(phone => phone.owner == lift(id)).delete)
      deletedPeople <- run(query[Person].filter(person => person.id == lift(id)).delete)
    } yield deletedPeople + deletedPhones
  }


  def updatePersonById(person: Person): ZIO[Any, SQLException, Long] = run {
    query[Person]
      .filter(_.id == lift(person.id))
      .updateValue(lift(person))
  }
}

object DataService {

  def main =
    for {
      _ <- ZIO.serviceWithZIO[DataService](_.initialize)

      people <- ZIO.serviceWithZIO[DataService](_.getPeople)
      _ <- printLine(s"Current people: $people")

      peoplePhones <- ZIO.serviceWithZIO[DataService](_.getPeoplePhones)
      _ <- printLine(s"Current peoplePhones: $peoplePhones")

      putPerson = Person(PersonId(4), "abc", Date.valueOf("2008-11-11"))
      result <- ZIO.serviceWithZIO[DataService](_.putPerson(putPerson))
      //.catchAll(_ => ZIO.succeed(()))
      _ <- printLine(s"Rows updated $result")
      result <- ZIO.serviceWithZIO[DataService](_.putPhone(Phone(None, "40", putPerson.id)))
      _ <- printLine(s"Phone id: $result")
      result <- ZIO.serviceWithZIO[DataService](_.putPhone(Phone(None, "41", putPerson.id)))
      _ <- printLine(s"Phone id: $result")

      people <- ZIO.serviceWithZIO[DataService](_.getPeople)
      _ <- printLine(s"People after put: $people")

      updatePerson = Person(putPerson.id, "cba", Date.valueOf("2008-11-12"))
      result <- ZIO.serviceWithZIO[DataService](_.updatePersonById(updatePerson))
      _ <- printLine(s"Rows updated $result")

      people <- ZIO.serviceWithZIO[DataService](_.getPeopleRaw)
      _ <- printLine(s"People after update: $people")

      result <- ZIO.serviceWithZIO[DataService](_.deletePersonById(putPerson.id))
      _ <- printLine(s"Rows updated $result")

      people <- ZIO.serviceWithZIO[DataService](_.getPeopleRaw)
      _ <- printLine(s"People after delete: $people")

      peoplePhones <- ZIO.serviceWithZIO[DataService](_.getPeoplePhones)
      _ <- printLine(s"Current peoplePhones: $peoplePhones")

    } yield ()

  val live = ZLayer.fromFunction(new DataService(_))
}



object Main extends ZIOAppDefault {
  override def run = {
    val ctxSqlite = new SqliteZioJdbcContext(SnakeCase)
    val ctxMysql = new MysqlZioJdbcContext(SnakeCase)

    DataService.main
      .provide(
        DataService.live,
        Quill.Mysql.fromNamingStrategy(SnakeCase),
        Quill.Sqlite.fromNamingStrategy(SnakeCase),
        //Quill.DataSource.fromPrefix("quill-mysql"),
        Quill.DataSource.fromPrefix("quill-sqlite"),
      )
      .debug("Result: ")
      .exitCode
  }
}