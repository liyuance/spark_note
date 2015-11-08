# Spark SQL源码分析

## Spark-Sql 整体流程
1. parseSQL(sql) => unresolvedLogicalPlan <: LogicalPlan
<br>    通过词法分析和语法分析将sql转化成Unresolved LogicalPlan。
2. analyzer.execute(unresolvedLogicalPlan) => resolvedLogicalPlan <: LogicalPlan
<br>    通过analyzer结合catalog对数据源进行绑定，生成resolved LogicalPlan
3. optimizer.execute(resolvedLogicalPlan) => optimizedLogicalPlan <: LogicalPlan
<br>    采用optimizer中定义的规则对resolved LogicalPlan进行优化，生成optimized LogicalPlan。
4. planner.plan(optimizedLogicalPlan).next() => sparkPlan <: SparkPlan
<br>    使用planner把logica planner转化成spark plan
5. prepareForExecution.execute(sparkPlan) => executedPlan <: SparkPlan
6. executedPlan.execute() => RDD[Row]
<br>    调用executedPlan的execute()，生成RDD返回。

其中unresolvedLogicalPlan、resolvedLogicalPlan和optimizedLogicalPlan都是LogicalPlan，sparkPlan和executedPlan是SparkPlan。

## SQLContext类
spark sql提供了两个sqlcontext，一个是默认是的SQLContext，另外一个是继承SQLContext的HiveContext。HiveContext重写了SQLContext部分方法和变量，为了整合hiveQL。

### 1.SQLContext主要成员方法和变量
<pre><code>
protected[sql] lazy val catalog: Catalog
protected[sql] lazy val functionRegistry: FunctionRegistry
protected[sql] lazy val analyzer: Analyzer
protected[sql] val ddlParser
protected[sql] val sqlParser
protected[sql] def dialectClassName  //DefaultParserDialect
protected[sql] def parseSql(sql: String): LogicalPlan
protected[sql] def executeSql(sql: String): this.QueryExecution
protected[sql] val tlSession  //对SQLSession进行ThreadLocal封装
protected[sql] class SQLSession //对SQLConf的封装
def sql(sqlText: String): DataFrame  //编译sql
protected[sql] class SparkPlanner extends SparkStrategies  
protected[sql] val planner = new SparkPlanner
protected[sql] val prepareForExecution  //
protected[sql] class QueryExecution(val logical: LogicalPlan)
object implicits  //rdd到Dataframe的隐式转化
~~~
load、jdbc、jsonFile等各种生成DataFrame的方法
~~~
</code></pre>

### 2.HiveContext重写的方法或者变量
<pre><code>
override protected[sql] lazy val catalog
override protected[sql] lazy val functionRegistry
override protected[sql] lazy val analyzer: Analyzer
protected[sql] override def parseSql(sql: String): LogicalPlan
override protected[sql] def dialectClassName  //HiveQLDialect
override protected[sql] def createSession(): SQLSession
private val hivePlanner = new SparkPlanner with HiveStrategies
override protected[sql] val planner = hivePlanner
</code></pre>

### 3.SQLContext vs HiveContext
1. catalog是用于数据绑定的字典，SQLContext中catalog=new SimpleCatalog(conf)，HiveContext中catalog=new HiveMetastoreCatalog(metadataHive, this)。
2. functionRegistry用于注册临时用户定义的临时函数SQLContext中functionRegistry=new SimpleFunctionRegistry(conf)，HiveContext中functionRegistry=new HiveFunctionRegistry
3. analyzer在两则中分别用各自的catalog和functionRegistry构建。analyzer = new Analyzer(catalog, functionRegistry, conf)
4. parseSql使用哪个sql解析器（ParseDialect）是由dialectClassName决定的，在SQLContext中dialectClassName是DefaultParserDialect，HiveContext中是HiveQLDialect。dialectClassName的名字是有参数spark.sql.dialect决定的，可以通过修改改变量选择不同sql解析器。
5. SQLContext使用使用SparkPlanner作为planner，planner = new SparkPlanner。HiveContex中planner = hivePlanner，hivePlanner = new SparkPlanner with HiveStrategies，HiveStrategies是trait，相对于SparkPlanner，hivePlanner增加了HiveCommandStrategy、HiveDDLStrategy等策略。

##Spark-shell创建SQLContext

~~~ 
  @DeveloperApi
  def createSQLContext(): SQLContext = {
    val name = "org.apache.spark.sql.hive.HiveContext"
    val loader = Utils.getContextOrSparkClassLoader
    try {
      sqlContext = loader.loadClass(name).getConstructor(classOf[SparkContext])
        .newInstance(sparkContext).asInstanceOf[SQLContext] 
      logInfo("Created sql context (with Hive support)..")
    }
    catch {
      case _: java.lang.ClassNotFoundException | _: java.lang.NoClassDefFoundError =>
        sqlContext = new SQLContext(sparkContext)
        logInfo("Created sql context..")
    }
    sqlContext
  }
~~~

- 用反射的方法先尝试创建HiveContext，如果失败则退化成SQLContext。所以，spark-shell是优先使用HiveContext。

## HiveContext.sql(sqlText: String)剖析
### 1.shell实例
<pre><code>
scala> sqlContext.sql("desc test_partition").collect.foreach(println)
[id,int,null]
[name,string,null]
[dt,string,null]
[# Partition Information,,]
[# col_name,data_type,comment]
[dt,string,null]
</code></pre>
表test_partition有id、name两个字段，dt为partition字段。

<pre><code>
scala> val query = sqlContext.sql("select name from test_partition where id > 0 limit 3")
query: org.apache.spark.sql.DataFrame = [name: string]
</code></pre>
sqlContext.sql()返回一个DataFrame

<pre><code>
scala> query.printSchema
root
 |-- name: string (nullable = true)
</code></pre>
打印出来的DataFrame的schema

<pre><code> 
scala> query.queryExecution
res8: org.apache.spark.sql.SQLContext#QueryExecution =
== Parsed Logical Plan ==
'Limit 3
 'Project ['name]
  'Filter ('id > 0)
   'UnresolvedRelation [test_partition], None

== Analyzed Logical Plan ==
name: string
Limit 3
 Project [name#29]
  Filter (id#28 > 0)
   MetastoreRelation xitong, test_partition, None

== Optimized Logical Plan ==
Limit 3
 Project [name#29]
  Filter (id#28 > 0)
   MetastoreRelation xitong, test_partition, None

== Physical Plan ==
Limit 3
 Project [name#29]
  Filter (id#28 > 0)
   HiveTableScan [name#29,id#28], (MetastoreRelation xitong, test_partition, None), None

Code Generation: false
== RDD ==
</code></pre>
打印整个QueryException，相当于query.queryExecution.logical + query.queryExecution.analyzed + query.queryExecution.optimizedPlan + query.queryExecution.sparkPlan + query.queryExecution.executedPlan 。
<br>    LogicalPlan和SparkPlan皆为树状结构，如：
<pre><code>
scala> query.queryExecution.logical
res16: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
'Limit 3
 'Project ['name]
  'Filter ('id > 0)
   'UnresolvedRelation [test_partition], None


scala> query.queryExecution.logical.children
res17: Seq[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] =
List('Project ['name]
 'Filter ('id > 0)
  'UnresolvedRelation [test_partition], None
)

scala> query.queryExecution.executedPlan
res20: org.apache.spark.sql.execution.SparkPlan =
Limit 3
 Project [name#29]
  Filter (id#28 > 0)
   HiveTableScan [name#29,id#28], (MetastoreRelation xitong, test_partition, None), None


scala> query.queryExecution.executedPlan.children
res21: Seq[org.apache.spark.sql.execution.SparkPlan] =
List(Project [name#29]
 Filter (id#28 > 0)
  HiveTableScan [name#29,id#28], (MetastoreRelation xitong, test_partition, None), None
)
</code></pre>

![sparkChildren](./res/sparkChildren.png)

### 2.sql(sqlText: String)代码分析
<pre><code>
  def sql(sqlText: String): DataFrame = {
    DataFrame(this, parseSql(sqlText))
  }
</code></pre>
sql()方法返回的是个DataFrame对象，传入构造函数的参数是本身（SQLContext）和parseSql()返回的LogicalPlan。其中parseSql(sqlText)中完成了sql的词法和语法分析工作。
<br>DataFrame的伴随Object定义如下：
<pre><code>
private[sql] object DataFrame {
  def apply(sqlContext: SQLContext, logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }
}
</code></pre>
DataFrame类的构造函数定义如下：
<pre><code>
  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan) = {
    this(sqlContext, {
      val qe = sqlContext.executePlan(logicalPlan)
      if (sqlContext.conf.dataFrameEagerAnalysis) {
        qe.assertAnalyzed()  // This should force analysis and throw errors if there are any
      }
      qe
    })
  }
</code></pre>
其中sqlContext.executePlan(logicalPlan)生成一个用于构建DataFrame的QueryExecution。详见代码：protected[sql] def executePlan(plan: LogicalPlan) = new this.QueryExecution(plan)。QueryExecution是SparkContext中的重要类，是对sql执行工作流的封装，利用它可以很方便的查看sql执行的每个过程的状态。QueryExecution类包含的主要成员如下代码所示：
<pre><code>
  protected[sql] class QueryExecution(val logical: LogicalPlan) {
    def assertAnalyzed(): Unit = analyzer.checkAnalysis(analyzed)
    //resolved逻辑计划
    lazy val analyzed: LogicalPlan = analyzer.execute(logical)
    lazy val withCachedData: LogicalPlan = {
      assertAnalyzed()
      cacheManager.useCachedData(analyzed)
    }
    //优化后的逻辑计划
    lazy val optimizedPlan: LogicalPlan = optimizer.execute(withCachedData)

    //物理执行计划
    lazy val sparkPlan: SparkPlan = {
      SparkPlan.currentContext.set(self)
      planner.plan(optimizedPlan).next()
    }
    //最终物理执行计划
    lazy val executedPlan: SparkPlan = prepareForExecution.execute(sparkPlan)

    /** Internal version of the RDD. Avoids copies and has no schema */
    lazy val toRdd: RDD[Row] = executedPlan.execute()
</code></pre>
QueryExecution处理的过程主要分为两个阶段，一个是LogicPlan处理阶段，另一个是PhysicalPlan处理阶段。LogicPlan处理阶段由analyzer和optimizer完成。PhysicalPlan处理阶段主要由planner完成。下面对parseSql、analyzer、optimizer和planner等组件进行依次分析。
## parseSql过程分析
<pre><code>
  protected[sql] def parseSql(sql: String): LogicalPlan = ddlParser.parse(sql, false)
  ...
  
  protected[sql] val ddlParser = new DDLParser(sqlParser.parse(_))
  ...
  
  protected[sql] val sqlParser = new SparkSQLParser(getSQLDialect().parse(_))
  
  protected[sql] def getSQLDialect(): ParserDialect = {
    try {
      val clazz = Utils.classForName(dialectClassName)
      clazz.newInstance().asInstanceOf[ParserDialect]
    } catch {
      case NonFatal(e) =>
        // Since we didn't find the available SQL Dialect, it will fail even for SET command:
        // SET spark.sql.dialect=sql; Let's reset as default dialect automatically.
        val dialect = conf.dialect
        // reset the sql dialect
        conf.unsetConf(SQLConf.DIALECT)
        // throw out the exception, and the default sql dialect will take effect for next query.
        throw new DialectException(
          s"""Instantiating dialect '$dialect' failed.
             |Reverting to default dialect '${conf.dialect}'""".stripMargin, e)
    }
  }
</code></pre>
上述代码中用dialectClassName类的初始化SparkParse，前面已经提到dialectClassName在HiveContex中为HiveQLDialect。HiveQLDialect的定义如下：
<pre><code>
private[hive] class HiveQLDialect extends ParserDialect {
  override def parse(sqlText: String): LogicalPlan = {
    HiveQl.parseSql(sqlText)
  }
}
</code></pre>
可见HiveContext的parseSql实际上使用的是HiveQl.parseSql(sqlText)做sql解析。HiveQl.parse的实现调用了ExtendedHiveQlParser中得parse()方法，如下：
<pre><code>
protected val hqlParser = new ExtendedHiveQlParser

/** Returns a LogicalPlan for a given HiveQL string. */
def parseSql(sql: String): LogicalPlan = hqlParser.parse(sql)
</code></pre>

ExtendedHiveQlParser继承与抽象类AbstractSparkSQLParser。AbstractSparkSQLParser的主要实现有ExtendedHiveQlParser、SqlParser、DDLParser、SparkSQLParser。默认的SQLContex中得DefaultParserDialect使用的是SqlParser。AbstractSparkSQLParser的继承关系如下：
<br>***此处有张图***<br>
具体sql的解析的入口是调用了AbstractSparkSQLParser.parse()函数，代码比较晦涩，展开看下：

<pre><code>
  def parse(input: String): LogicalPlan = {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError => sys.error(failureOrError.toString)
    }
  }
</code></pre>
重点是“phrase(start)(new lexical.Scanner(input))”这句代码。意思是将输入的sql字符串调用lexical词法分析器进行分析，分析的结果传给start语法分析器进行分析。在此之前要先对lexical进行初始化，初始化的主要工作室初始化Keywords列表,Initialize的定义如下：
<pre><code>
protected lazy val initLexical: Unit = lexical.initialize(reservedWords)。
</code></pre>
下面分别看下lexical、start、Keyword是如何定义的，以及Keywords如何获取。lexical是一个SqlLexical对象，声明及部分代码如下：
<pre><code>
override val lexical = new SqlLexical
/* This is a work around to support the lazy setting */
def initialize(keywords: Seq[String]): Unit = {
  reserved.clear()
  reserved ++= keywords
}
/* Normal the keyword string */
def normalizeKeyword(str: String): String = str.toLowerCase
</code></pre>
语法分析器start的具体定义在AbstractSparkSQLParser的子类中完成，以ExtendedHiveQlParser为例，其相关代码如下：
<pre><code>
private[hive] class ExtendedHiveQlParser extends AbstractSparkSQLParser {
  // Keyword is a convention with AbstractSparkSQLParser, which will scan all of the `Keyword`
  // properties via reflection the class in runtime for constructing the SqlLexical object
  protected val ADD = Keyword("ADD")
  protected val DFS = Keyword("DFS")
  protected val FILE = Keyword("FILE")
  protected val JAR = Keyword("JAR")

  protected lazy val start: Parser[LogicalPlan] = dfs | addJar | addFile | hiveQl

  protected lazy val hiveQl: Parser[LogicalPlan] =
    restInput ^^ {
      case statement => HiveQl.createPlan(statement.trim)
    }

  protected lazy val dfs: Parser[LogicalPlan] =
    DFS ~> wholeInput ^^ {
      case command => HiveNativeCommand(command.trim)
    }

  private lazy val addFile: Parser[LogicalPlan] =
    ADD ~ FILE ~> restInput ^^ {
      case input => AddFile(input.trim)
    }

  private lazy val addJar: Parser[LogicalPlan] =
    ADD ~ JAR ~> restInput ^^ {
      case input => AddJar(input.trim)
    }
}
</code></pre>
代码中定义了四个关键词分别用于"ADD JAR"、"ADD FILE"、"DFS"相关的hive内部指令。start的定义为“dfs | addJar | addFile | hiveQl”，即顺序使用四个语法规则进行匹配。由代码可知当配到dfs词法时将“wholeInput”（整个输入）封装成HiveNativeCommand。如果匹配到 ADD 后边跟随FILE的词法时将“restInput”剩余输入封装成AddFile，AddJarl类似。如果都没有匹配则最后交给HiveQl.createPlan()方法进行处理。其中HiveNativeCommand、AddFile、和AddJar都继承与RunnableCommand。RunnableCommand是LogicalPlan的子类。
<br>词法分析器的Keywords构建方法是通过lexical.initialize(reservedWords)完成，其中reservedWords的定义如下：
<pre><code>
protected lazy val reservedWords: Seq[String] =
  this
    .getClass
    .getMethods
    .filter(_.getReturnType == classOf[Keyword])
    .map(_.invoke(this).asInstanceOf[Keyword].normalize)
</code></pre>
通过反射的方法获取子类中定义的所有Keyword对象，并提取Keyword名后转换成小写。Keyword类的定义如下：
<pre><code>
  protected case class Keyword(str: String) {
    def normalize: String = lexical.normalizeKeyword(str)
    def parser: Parser[String] = normalize
  }
</code></pre>
至此可知，对于hiveSql中得“dfs、add file、add jar”都封装成了RunnableCommand，其他sql还要由HiveQl.createPlan继续分析。下面走进HiveQl.createPlan的代码。核心代码如下：
<pre><code>
/** Creates LogicalPlan for a given HiveQL string. */
def createPlan(sql: String): LogicalPlan =
  ...
  val tree = getAst(sql)
  if (nativeCommands contains tree.getText) {
    HiveNativeCommand(sql)
  } else {
    nodeToPlan(tree) match {
      case NativePlaceholder => HiveNativeCommand(sql)
      case other => other
    }
  }
  ...
}
</code></pre>
其中getAst(sql)方法是生成sql对应的AST语法树tree。然后根据nativeCommands和tree决定创建HiveNativeCommand或者其他LogicPlan。nativeCommands是预定义的一个语法树类型列表，主要包括一些修改hive表、查看hive表元信息、权限管理等。
<br>getAst(sql)生成语法树的过程是通过调用hive的ParseDriver中的parse(sql, hiveContext)完成的，具体代码如下：
<pre><code>
  /**
   * Returns the AST for the given SQL string.
   */
  def getAst(sql: String): ASTNode = {
    val hContext = new Context(hiveConf)
    val node = ParseUtils.findRootNonNullToken((new ParseDriver).parse(sql, hContext))
    hContext.clear()
    node
  }
</code></pre>
## analyzer分析
analyzer主要完成绑定工作，将不同来源的Unresolved LogicalPlan和数据元数据（如hive metastore）进行绑定，生成resolved LogicalPlan。
<br>analyzer和optimizer都是RuleExecutor的子类。对于RuleExecutor的子类来讲最重要的就是定义自己的batches，而execute()方法就是依次在传入的plan中依次应用这个规则。analyer中定义的batches如下：
<pre><code>
  lazy val batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      CTESubstitution ::
      WindowsSubstitution ::
      Nil : _*),
    Batch("Resolution", fixedPoint,
      ResolveRelations ::
      ResolveReferences ::
      ResolveGroupingAnalytics ::
      ResolveSortReferences ::
      ResolveGenerate ::
      ResolveFunctions ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      UnresolvedHavingClauseAttributes ::
      TrimGroupingAliases ::
      typeCoercionRules ++
      extendedResolutionRules : _*)
  )
</code></pre>
其中ResolveRelations就是为了解决sql中数据源映射，如从一个表名查找到表的实际存储位置。ResolveRelations的定义如下：
<pre><code>
  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelations extends Rule[LogicalPlan] {
    def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"no such table ${u.tableName}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case i@InsertIntoTable(u: UnresolvedRelation, _, _, _, _) =>
        i.copy(table = EliminateSubQueries(getTable(u)))
      case u: UnresolvedRelation =>
        getTable(u)
    }
  }
</code></pre>
Rule的默认处理函数apply()调用了getTable()方法，在getTable()方法中通过catalog的lookupRelation()方法完成对数据源的查找。catalog子类的各自lookupRelation()方法的实现不同，在HiveContext中catalog是HiveMetastoreCatalog的实例，HiveMetastoreCatalog中的lookupRelation()方法定义如下：
<pre><code>
  def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String]): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val databaseName = tableIdent.lift(tableIdent.size - 2).getOrElse(
      client.currentDatabase)
    val tblName = tableIdent.last
    val table = client.getTable(databaseName, tblName)

    if (table.properties.get("spark.sql.sources.provider").isDefined) {
      val dataSourceTable =
        cachedDataSourceTables(QualifiedTableName(databaseName, tblName).toLowerCase)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Otherwise, wrap the table with a Subquery using the table name.
      val withAlias =
        alias.map(a => Subquery(a, dataSourceTable)).getOrElse(
          Subquery(tableIdent.last, dataSourceTable))

      withAlias
    } else if (table.tableType == VirtualView) {
      val viewText = table.viewText.getOrElse(sys.error("Invalid view without text."))
      alias match {
        // because hive use things like `_c0` to build the expanded text
        // currently we cannot support view from "create view v1(c1) as ..."
        case None => Subquery(table.name, HiveQl.createPlan(viewText))
        case Some(aliasText) => Subquery(aliasText, HiveQl.createPlan(viewText))
      }
    } else {
      MetastoreRelation(databaseName, tblName, alias)(table)(hive)
    }
  }
</code></pre>
其中client是ClientWrapper的实例，通过client的getTable()方法获取对应的HiveTable实例。getTable()方法又调用了其内部的getTableOption()方法，进而通过org.apache.hadoop.hive.ql.metadata.Hive的getTable()方法完成的。
## optimizer分析

## planner分析

## SparkPlan.execute()分析

## 总结

## TODO

