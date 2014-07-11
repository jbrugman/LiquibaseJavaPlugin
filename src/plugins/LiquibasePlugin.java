package plugins;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import liquibase.Liquibase;
import liquibase.changelog.ChangeSet;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.FileSystemResourceAccessor;
import models.utils.exception.EviewLogger;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import play.Application;
import play.Configuration;
import play.Logger;
import play.Play;
import play.Plugin;
import play.api.PlayException;
import play.api.db.DBPlugin;
import play.api.mvc.RequestHeader;
import play.api.mvc.SimpleResult;
import play.core.*;
import scala.Option;
import scala.Tuple2;

import static play.mvc.Controller.*;

/**
 * @date: 01.07.14
 * @authors: Stijn Beekhuis & Justus Brugman
 */
public class LiquibasePlugin extends Plugin implements HandleWebCommandSupport {

  private static final String TEST_CONTEXT = "test";
  private static final String DEVELOPMENT_CONTEXT = "dev";
  private static final String PRODUCTION_CONTEXT = "prod";
  private static final String LIQUIBASE_APPLY_DENOTER = "applyLiquibase";

  private Application application;
  private String masterChangelogFile;
  private String masterTempChangelogFile;
  private String className;

  /**
   * Constructor
   *
   * @param application
   */
  public LiquibasePlugin(Application application) {
    super();
    setApplication(application);
    setClassName(this.getClass().getSimpleName());
  }

  /**
   * Start the liquibase scripts on starting the application
   */
  @Override
  public void onStart() {
    Logger.info(getClassName() + " starting ...");
    scala.collection.immutable.List<Tuple2<DataSource, String>> datasources = Play.application().plugin(DBPlugin.class)
      .api().datasources();

    for (int i = 0; i < datasources.length(); i++) {
      DataSource dataSource = datasources.apply(i)._1();
      String dataSourceName = datasources.apply(i)._2();

      setMasterChangelogFile(application.path().getAbsolutePath() + "/conf/liquibase/" + dataSourceName
        + "/db.changelog-master.xml");
      setMasterTempChangelogFile(this.getMasterChangelogFile() + "_tmp.xml");

      checkOrCreateContentChangelogTable(dataSource);
      compareAndManageChangeLogFiles(dataSource, false);

      if (shouldApplyForDataSource(dataSourceName)) {
        Liquibase liquibase = createLiquibaseInstance(getMasterChangelogFile(), dataSource);

        if (isChangeSetsAvailable(liquibase)) {
          // Store unique filenames of changesets.
          List<String> changeFiles = getChangeLogFilenames(liquibase);

          if (Play.isProd()) {
            throw new PlayException("No auto update to production database", getClassName() + " "
              + "Production database needs updates. Please " + "run manually.");
          } else {
            String tag = tagCurrentState(liquibase);

            if (testRollback(liquibase)) {
              if (!update(liquibase)) {
                rollback(liquibase, tag);

                throw new PlayException(getClassName() + " Update error", "Liquibase did not perform update from " +
                  "changesets.");
              }
            } else {
              rollback(liquibase, tag);
              throw new PlayException(getClassName() + " Test error", "Liquibase did not perform update from changesets.");
            }
            // Store backup of files in the database
            for (String changed : changeFiles) {
              createContentChangelogItem(dataSource, changed, tag, liquibase);
            }
          }
        } else {
          EviewLogger.info(getClassName() + " Liquibase did not find any changesets.");
        }
      } else {
        EviewLogger.warn(getClassName() + " Apply Liquibase disabled in configuration for datasource: " + dataSourceName);
      }
    }
    EviewLogger.info(getClassName() + " finished (ok)");
  }

  /**
   * This service should always start.
   *
   * @return
   */
  @Override
  public boolean enabled() {
    return true;
  }

  /**
   * create new Liquibase instance with changeLogFile and datasource
   *
   * @param changeLogFile
   * @param dataSource
   * @return Liquibase instance
   */
  private Liquibase createLiquibaseInstance(String changeLogFile, DataSource dataSource) {
    Liquibase liquiBase;
    try {
      liquiBase = new Liquibase(changeLogFile,
        new FileSystemResourceAccessor(getApplication().path().getAbsolutePath()), new JdbcConnection(
        dataSource.getConnection()));
    } catch (LiquibaseException e) {
      throw new PlayException(getClassName() + " cannot create Liquibase instance", e.getMessage());
    } catch (SQLException e) {
      throw new PlayException(getClassName() + " cannot create Liquibase instance", e.getMessage());
    }
    return liquiBase;
  }

  /**
   * @param liquibase
   * @return whether there are changesets
   */
  private boolean isChangeSetsAvailable(Liquibase liquibase) {
    boolean changeSetsAvailable;
    try {
      changeSetsAvailable = !liquibase.listUnrunChangeSets(getContext()).isEmpty();
    } catch (LiquibaseException e) {
      throw new PlayException(getClassName() + " cannot get list of changesets", e.getMessage());
    }
    return changeSetsAvailable;
  }

  /**
   * tags the current state
   *
   * @param liquibase
   * @return the new tag
   */
  private String tagCurrentState(Liquibase liquibase) {
    String tag = getTag();
    try {
      EviewLogger.info(getClassName() + " Liquibase tagging current state ...");
      liquibase.tag(tag);
      EviewLogger.info(getClassName() + " Liquibase tagging current state (ok)");
      return tag;
    } catch (LiquibaseException e) {
      throw new PlayException(getClassName() + " cannot tag current state", e.getMessage());
    }
  }

  /**
   * tests if the changes can be rolled back
   *
   * @param liquibase
   * @return
   */
  private boolean testRollback(Liquibase liquibase) {
    try {
      EviewLogger.info(getClassName() + " Liquibase test rollback changesets ...");
      liquibase.updateTestingRollback(getContext());
      EviewLogger.info(getClassName() + " Liquibase test rollback changesets (ok)");
      return true;
    } catch (LiquibaseException e) {
      EviewLogger.warn(getClassName() + " Liquibase test rollback changesets (FAIL)");
      return false;
    }
  }

  /**
   * rolls back to latest tag;
   *
   * @param liquibase
   * @param tag
   */
  private void rollback(Liquibase liquibase, String tag) {
    try {
      EviewLogger.info(getClassName() + " Liquibase trying rollback to tag: " + tag);
      liquibase.rollback(tag, getContext());
      EviewLogger.info(getClassName() + " Liquibase trying rollback (ok)");
    } catch (LiquibaseException e) {
      throw new PlayException(getClassName() + " cannot rollback changeset", e.getMessage());
    }
  }

  /**
   * updates database with changesets
   *
   * @param liquibase
   * @return
   */
  private boolean update(Liquibase liquibase) {
    try {
      EviewLogger.info(getClassName() + " Liquibase applying changesets ...");
      liquibase.update(getContext());
      EviewLogger.info(getClassName() + " Liquibase applying changesets (ok)");
      return true;
    } catch (LiquibaseException e) {
      EviewLogger.warn(getClassName() + " Liquibase applying changesets (FAIL)");
      EviewLogger.error(getClassName() + " Liquibase applying changesets error", e);
      return false;
    }
  }

  /**
   * @return generates tag
   */
  private String getTag() {
    return "state-" + DateTime.now().toString(DateTimeFormat.forPattern("yyyy-M-d hh:mm:ss"));
  }

  /**
   * @param dataSource
   * @return true or false whether or not to apply changelog files
   */
  private boolean shouldApplyForDataSource(String dataSource) {
    Boolean apply = Configuration.root().getBoolean(LIQUIBASE_APPLY_DENOTER + "." + dataSource);
    return apply instanceof Boolean ? apply.booleanValue() : false;
  }

  /**
   * getContext
   *
   * @return context string from play mode
   */
  private String getContext() {
    if (Play.isDev()) {
      return DEVELOPMENT_CONTEXT;
    } else if (Play.isProd()) {
      return PRODUCTION_CONTEXT;
    } else if (Play.isTest()) {
      return TEST_CONTEXT;
    } else {
      throw new PlayException("Play context exception", "No context specified");
    }
  }

  /**
   * Read the single filenames of the unrun changesets.  (Return them as ordered list)
   *
   * @param liquibase
   * @return
   */
  private List getChangeLogFilenames(Liquibase liquibase) {
    try {
      List<String> changeFiles = new ArrayList<String>();
      for (ChangeSet changeSet : liquibase.listUnrunChangeSets(getContext())) {
        if (!changeFiles.contains(changeSet.getFilePath())) {
          changeFiles.add(changeSet.getFilePath());
        }
      }
      return changeFiles;
    } catch (LiquibaseException e) {
      throw new PlayException(getClassName() + " cannot get list of changesets", e.getMessage());
    }
  }

  /**
   * Add changelog item to database log
   *
   * @param dataSource
   * @param filename
   * @param tag
   * @param liquibase
   */
  private void createContentChangelogItem(DataSource dataSource, String filename, String tag, Liquibase liquibase) {
    try {
      JdbcConnection conn = new JdbcConnection(dataSource.getConnection());
      String content = new String(Files.readAllBytes(Paths.get(filename)), Charset.defaultCharset());

      PreparedStatement p = conn.prepareStatement("Insert into CONTENTCHANGELOG (filename,date,content,tag) " +
        "values (?,?,?,?) ");

      p.setString(1, filename);
      p.setString(2, DateTime.now().toString(DateTimeFormat.forPattern("yyyy-M-d hh:mm:ss")));
      p.setString(3, content);
      p.setString(4, tag);
      p.execute();

      EviewLogger.info(getClassName() + " Cobtent inserted changelog (ok)");

    } catch (SQLException e) {
      rollback(liquibase, tag);
      throw new PlayException(getClassName() + " SQL error", e.getMessage());
    } catch (DatabaseException e) {
      rollback(liquibase, tag);
      throw new PlayException(getClassName() + " Database error", e.getMessage());
    } catch (IOException e) {
      rollback(liquibase, tag);
      throw new PlayException(getClassName() + " File not found error", e.getMessage());
    }
  }

  /**
   * Checks if the Content changelog table exists. If not, create it.
   *
   * @param dataSource
   */
  private void checkOrCreateContentChangelogTable(DataSource dataSource) {
    if (doesTableExist("CONTENTCHANGELOG", dataSource)) {
      EviewLogger.info(getClassName() + "   Changelog table exists...");
    } else {
      createContentChangelogTable(dataSource);
      EviewLogger.info(getClassName() + " Created   Changelog table...");
    }
  }

  /**
   * Check if a table exists in the database.
   *
   * @param tableName
   * @param dataSource
   * @return
   */
  private boolean doesTableExist(String tableName, DataSource dataSource) {
    try {
      JdbcConnection conn = new JdbcConnection(dataSource.getConnection());
      DatabaseMetaData dbm = conn.getMetaData();
      ResultSet tableSet = dbm.getTables(null, null, tableName, null);
      if (tableSet.next()) {
        return true;
      } else {
        return false;
      }

    } catch (SQLException e) {
      throw new PlayException(getClassName() + " SQL error", e.getMessage());
    } catch (DatabaseException e) {
      throw new PlayException(getClassName() + " Database error", e.getMessage());
    }
  }

  /**
   * Create the Changelog table
   *
   * @param dataSource
   */
  private void createContentChangelogTable(DataSource dataSource) {
    try {
      JdbcConnection conn = new JdbcConnection(dataSource.getConnection());

      String sqlCreate = "CREATE TABLE CONTENTCHANGELOG ("
        + " id              bigint auto_increment not null,"
        + " filename        varchar(255),"
        + " date            datetime,"
        + " content         longtext,"
        + " tag             varchar(255),"
        + " constraint pk_contentchangelog primary key (id))";

      Statement stmt = conn.createStatement();
      stmt.execute(sqlCreate);

    } catch (SQLException e) {
      throw new PlayException(getClassName() + " SQL error", e.getMessage());
    } catch (DatabaseException e) {
      throw new PlayException(getClassName() + " Database error", e.getMessage());
    }
  }

  /**
   * Checks if a file exists.
   *
   * @param filename
   * @return
   */
  private boolean doesFileExist(String filename) {
    File f = new File(filename);
    if (f.exists() && !f.isDirectory()) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Recreate the logfile that was missing
   *
   * @param filename
   * @param content
   */
  private void generateFile(String filename, String content) {
    try {
      if (doesFileExist(filename)) {
        throw new PlayException(getClassName() + " Error!", "The file " + filename + " exists!");
      }
      FileWriter file = new FileWriter(filename);
      file.write(content);
      file.close();
      EviewLogger.info(getClassName() + " Created: " + filename);
    } catch (IOException e) {
      throw new PlayException(getClassName() + " IO error", e.getMessage());
    }
  }

  /**
   * Create a temporary master file to rollback last changes.
   *
   * @param includeFileName
   */
  private void createTempMasterFile(String includeFileName) {
    try {
      String content = new String(Files.readAllBytes(Paths.get(getMasterChangelogFile())), Charset.defaultCharset());
      String tempContent = content.replace("</databaseChangeLog>", "<include file=\"" + includeFileName + "\"/>");
      tempContent = tempContent + "</databaseChangeLog>";

      generateFile(getMasterTempChangelogFile(), tempContent);

    } catch (IOException e) {
      throw new PlayException(getClassName() + " IO error", e.getMessage());
    }
  }

  /**
   * Delete generated temp file from filesystem
   *
   * @param filename
   */
  private void cleanUpTempFile(String filename) {
    File file = new File(filename);
    if (!file.delete()) {
      throw new PlayException("Error", getClassName() + " Cannot delete " + filename);
    }
  }

  /**
   * Rename a file
   * The renameTo method is not reliable;
   * see http://stackoverflow.com/questions/1000183/reliable-file-renameto-alternative-on-windows
   *
   * @param oldName
   * @param newName
   * @throws IOException
   */
  public static void renameFile(String oldName, String newName) throws IOException {
    File srcFile = new File(oldName);
    boolean bSucceeded = false;
    try {
      File destFile = new File(newName);
      if (destFile.exists()) {
        if (!destFile.delete()) {
          throw new IOException(oldName + " was not successfully renamed to " + newName);
        }
      }
      if (!srcFile.renameTo(destFile)) {
        throw new IOException(oldName + " was not successfully renamed to " + newName);
      } else {
        bSucceeded = true;
      }
    } finally {
      if (bSucceeded) {
        EviewLogger.info("Liquibase service - Renamed " + oldName + " to " + newName);
        srcFile.delete();
      }
    }
  }

  /**
   * Compares the found set of changelog files to the set defined in the database
   * Creates missing logfiles and rolls them back as needed.
   *
   * @param dataSource
   */
  private void compareAndManageChangeLogFiles(DataSource dataSource, boolean executeRollback) {
    try {
      if (doesTableExist("DATABASECHANGELOG", dataSource)) {
        JdbcConnection conn = new JdbcConnection(dataSource.getConnection());

        String lqbCheckSql = "SELECT DISTINCT filename FROM DATABASECHANGELOG WHERE filename != 'liquibase-internal' " +
          "ORDER BY ORDEREXECUTED DESC";

        PreparedStatement contentStatement = conn.prepareStatement("SELECT id, content, " +
          "tag FROM CONTENTCHANGELOG WHERE filename = ?");

        PreparedStatement contentCleanup = conn.prepareStatement("DELETE FROM CONTENTCHANGELOG WHERE filename = ?");

        Statement lqbStmt = conn.createStatement();
        lqbStmt.execute(lqbCheckSql);
        ResultSet lqbResultSet = lqbStmt.getResultSet();

        while (lqbResultSet.next()) {
          String filename = lqbResultSet.getString("filename");
          if (!doesFileExist(filename)) {
            EviewLogger.warn(getClassName() + " Missing changelog file: " + filename);

            contentStatement.setString(1, filename);
            contentStatement.execute();
            ResultSet contentResultSet = contentStatement.getResultSet();
            if (contentResultSet.next()) {

              if (!executeRollback) {
                throw new CustomException("Database rollback action needed", filename);
              }

              // Create temporary logfile and master-file...
              generateFile(filename, contentResultSet.getString("content"));
              createTempMasterFile(filename);

              // Rolling back liquibase changeset...
              Liquibase liquibase = createLiquibaseInstance(getMasterTempChangelogFile(), dataSource);
              rollback(liquibase, contentResultSet.getString("tag"));

              // Cleaning up files and database
              cleanUpTempFile(filename);
              cleanUpTempFile(getMasterTempChangelogFile());
              contentCleanup.setString(1, filename);
              contentCleanup.execute();

            } else {
              throw new PlayException("Error", getClassName() + " Cannot regenerate " + filename);
            }
          } else {
            // Check if changelogFile content equals the content of the db.
            String content = new String(Files.readAllBytes(Paths.get(filename)), Charset.defaultCharset());

            contentStatement.setString(1, filename);
            contentStatement.execute();
            ResultSet contentResultSet = contentStatement.getResultSet();
            if (contentResultSet.next()) {
              if (!contentResultSet.getString("content").equals(content)) {
                EviewLogger.info(getClassName() + " Changed changeset, rolling back so we can replace it.");
                // Rename file
                renameFile(filename, filename + ".tmp");

                // Create logfile
                generateFile(filename, contentResultSet.getString("content"));

                // Rolling back old changeset
                Liquibase liquibase = createLiquibaseInstance(getMasterChangelogFile(), dataSource);
                rollback(liquibase, contentResultSet.getString("tag"));

                // Cleaning up files and database
                cleanUpTempFile(filename);
                contentCleanup.setString(1, filename);
                contentCleanup.execute();

                // Rename the new file back
                renameFile(filename + ".tmp", filename);
              }
            } else {
              EviewLogger.info(getClassName() + " No changes for " + filename);
            }
          }
        }

      } else {
        // Nothing to do, the Liquibase changelog table does not yet exist. (clean run)
        EviewLogger.info(getClassName() + " First time run, no compare needed.");
      }
    } catch (SQLException e) {
      throw new PlayException(getClassName() + " SQL error", e.getMessage());
    } catch (DatabaseException e) {
      throw new PlayException(getClassName() + " Database error", e.getMessage());
    } catch (IOException e) {
      throw new PlayException(getClassName() + " IO error", e.getMessage());
    }
  }

  // Getters and setters
  public Application getApplication() {
    return application;
  }

  public void setApplication(Application application) {
    this.application = application;
  }

  public String getMasterChangelogFile() {
    return masterChangelogFile;
  }

  public void setMasterChangelogFile(String masterChangelogFile) {
    this.masterChangelogFile = masterChangelogFile;
  }

  public String getMasterTempChangelogFile() {
    return masterTempChangelogFile;
  }

  public void setMasterTempChangelogFile(String masterTempChangelogFile) {
    this.masterTempChangelogFile = masterTempChangelogFile;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  @Override
  public Option<SimpleResult> handleWebCommand(RequestHeader request, SBTLink sbtLink, File path) {
    if (request.path().equals("/@liquibase/execute/rollback")) {

      scala.collection.immutable.List<Tuple2<DataSource, String>> datasources = Play.application().plugin(DBPlugin.class)
        .api().datasources();

      for (int i = 0; i < datasources.length(); i++) {
        DataSource dataSource = datasources.apply(i)._1();
        String dataSourceName = datasources.apply(i)._2();

        setMasterChangelogFile(application.path().getAbsolutePath() + "/conf/liquibase/" + dataSourceName
          + "/db.changelog-master.xml");
        setMasterTempChangelogFile(this.getMasterChangelogFile() + "_tmp.xml");

        checkOrCreateContentChangelogTable(dataSource);
        compareAndManageChangeLogFiles(dataSource, true);
      }

      sbtLink.forceReload();
      return Option.apply(redirect("/").getWrappedSimpleResult());
    }
    return Option.<SimpleResult>empty();
  }
}

class CustomException extends PlayException.RichDescription {

  public CustomException(String title, String description) {
    super(title, description);
  }

  public CustomException(String title, String description, Throwable cause) {
    super(title, description, cause);
  }

  @Override
  public String htmlDescription() {
    return "<span>I need to rollback some files on the database...</span>" +
      "<input name=\"button\" type=\"button\" value=\"Do it!\" onclick=\"{document.location = " +
      "'/@liquibase/execute/rollback'}\" />";
  }

  @Override
  public String subTitle() {
    return "Last changelog file";
  }

  @Override
  public String content() {
    return description;
  }
}

