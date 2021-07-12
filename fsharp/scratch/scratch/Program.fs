// Learn more about F# at http://docs.microsoft.com/dotnet/fsharp

open System
open System.Collections.Generic
open System.Data.SqlClient

// Define a function to construct a message to print

open System.IO
open System.Text
open System.Text.RegularExpressions
open System.Threading.Tasks
open Dapper

type BatchCount =
    { startLine: int
      endLine: int
      batch: string }

/// As LogAbility can be subscoped, we need to pass it at each function,
/// it's not possible to pass it at constructors
type LogAbility private (parent: Option<LogAbility>, props: IDictionary<string, string>) =

    static let _scope parent props scoped = scoped (LogAbility(parent, props))

    member this.subScope = Some(this) |> _scope

    static member scope = None |> _scope

    member this.logMessage(message: String) = this.log "message" message

    member private _.doLog(builder: StringBuilder) =
        match parent with
        | None -> ()
        | Some (parent) -> parent.doLog builder

        for kv in props do
            builder.Append($"{kv.Key}: «{kv.Value}» ")
            |> ignore

    member this.log (name: String) (message: String) =
        let builder = StringBuilder()
        this.doLog builder
        builder.Append($"{name}: {message}") |> ignore
        printf $"{builder}{Environment.NewLine}"

type ConnectAbility private () =
    let mutable connection : SqlConnection = null

    static member scope scoped =
        let connectAbility = ConnectAbility()
        let result = scoped connectAbility
        connectAbility.dispose ()
        result

    member this.configure(connString) =
        if isNull connection then
            connection <- new SqlConnection(connString)
            connection.Open()
            connection
        else if connection.ConnectionString = connString then
            connection
        else
            connection.Dispose()
            connection <- null
            this.configure connString

    member this.connect() =
        if isNull connection then
            raise (Exception("Not configured connection"))
        else
            connection

    member private this.dispose() =
        connection.Dispose()
        connection <- null



let keywordUse = "USE"
let keywordBegin = "BEGIN TRANSACTION"
let keywordCommit = "COMMIT TRANSACTION"

type TransactAbility private (connectAbility: ConnectAbility) =
    let mutable transaction : SqlTransaction = null

    static member scope scoped connectAbility =
        let transactAbility = TransactAbility(connectAbility)
        let result = scoped transactAbility
        transactAbility.dispose ()
        result

    member this.dispose() =
        transaction.Dispose()
        transaction <- null

    member this.configureConnection connString =
        connectAbility.configure connString |> ignore

    member this.beginTransaction(logAbility: LogAbility) =
        if transaction = null then
            transaction <- connectAbility.connect().BeginTransaction()
            logAbility.logMessage keywordBegin

    member this.execute command =
        connectAbility
            .connect()
            .Execute(command, null, transaction)
        |> ignore

    member this.commitTransaction(logAbility: LogAbility) =
        transaction.Commit()
        transaction <- null
        logAbility.logMessage keywordCommit

let finalConnString connPrefix (db: option<String>) =
    match db with
    | None -> connPrefix
    | Some (dbName) -> $"{connPrefix};Initial Catalog={dbName}"

let getDb batchCount =
    Some(
        batchCount
            .batch
            .Substring(keywordUse.Length)
            .Trim('\r', '\n', '[', ']', ' ')
    )

let getExecutedMessage i batchCount =
    let linesRange =
        $"[lines {batchCount.startLine}..{batchCount.endLine}]"

    $"batch #{i + 1} {linesRange}"

let executeCommand connPrefix batchCount (transactAbility: TransactAbility) (logAbility: LogAbility) =
    if batchCount.batch.StartsWith(keywordUse) then
        let db = getDb batchCount
        let connString = finalConnString connPrefix db
        transactAbility.configureConnection connString
        logAbility.logMessage $"Changed connection to db {db}"
    else if batchCount.batch.Trim().ToUpper() = keywordBegin then
        transactAbility.beginTransaction logAbility
    else if batchCount.batch.Trim().ToUpper() = keywordCommit then
        transactAbility.commitTransaction logAbility
    else
        let lines =
            batchCount.endLine - batchCount.startLine

        transactAbility.execute batchCount.batch
        logAbility.log "lines" $"{lines}"

let tryExecuteCommand connPrefix batchCount transactAbility logAbility =
    try
        executeCommand connPrefix batchCount transactAbility logAbility
    with
    | :? SqlException as ex -> logAbility.log "Exception" $"({ex.Message}) -> {batchCount.batch}"

let executeCommands connPrefix (commands: seq<BatchCount>) (transactAbility: TransactAbility) (logAbility: LogAbility) =
    commands
    |> Seq.iteri
        (fun i batchCount ->
            let executed = getExecutedMessage i batchCount

            logAbility.subScope (dict [ "executed", executed ]) (tryExecuteCommand connPrefix batchCount transactAbility))

let delim = "GO"

let getBatches (lines: seq<String>) =
    let mutable builder = StringBuilder()
    let mutable startLine = 0
    let mutable endLine = 0

    let getBatchCount () =
        { startLine = startLine + 1
          endLine = endLine
          batch = builder.ToString() }

    seq {
        for line in lines do
            endLine <- endLine + 1

            if line = delim then
                yield getBatchCount ()
                startLine <- endLine
                builder.Clear() |> ignore
            else
                builder.AppendLine(line) |> ignore

        if builder.Length <> 0 then
            yield getBatchCount ()
    }

let ALPHANUMERIC = "[a-zA-Z0-9_]+"

let CREATE_DATABASE_REGEX =
    Regex($"CREATE DATABASE \[(?<dbName>{ALPHANUMERIC})\]")

let SET_IDENTIY_INSERT_ON_REGEX =
    Regex($"SET IDENTITY_INSERT \[.+\]\.\[.+\] ON")

let SET_IDENTIY_INSERT_OFF_REGEX =
    Regex($"SET IDENTITY_INSERT \[.+\]\.\[.+\] OFF")

let transformBatches (batches: seq<BatchCount>) =
    seq {
        for batchCount in batches do
            let _match =
                CREATE_DATABASE_REGEX.Match(batchCount.batch)

            let withBatch batch = { batchCount with batch = batch }

            if _match.Success then
                let dbName =
                    _match.Groups.["dbName"].Captures.[0].Value

                yield withBatch $"CREATE DATABASE [{dbName}]"
            else if SET_IDENTIY_INSERT_ON_REGEX.IsMatch(batchCount.batch) then
                yield withBatch keywordBegin
                yield batchCount
            else if SET_IDENTIY_INSERT_OFF_REGEX.IsMatch(batchCount.batch) then
                yield batchCount
                yield withBatch keywordCommit
                yield withBatch keywordBegin
            else
                yield batchCount
    }


[<EntryPoint>]
let main argv =
    let connString =
        "Data Source=localhost;User Id=sa;Password=aSdA56ss652;"

    let getFileName (file: String) =
        $"\\\\hcch.com\DFS\Home\Houston\GGOMEZ\SQL Server Management Studio\Scripts\\{file}"

    let program =
        getFileName
        >> File.ReadLines
        >> getBatches
        >> transformBatches
        >> executeCommands connString

    let withAbilities file scoped =
        scoped >> LogAbility.scope (dict [ "file", file ])
        |> TransactAbility.scope
        |> ConnectAbility.scope

    let programWithAbilities file = (program file) |> (withAbilities file)

    let startNew task state =
        Task.Factory.StartNew(fun () -> task state)

    let files =
        [| "Kenrick_Infinity.sql"
           "wk_reporting_Infinity.sql"
           "CMS_Data_Infinity.sql" |]

    let tasks =
        files
        |> Seq.map (programWithAbilities |> startNew)

    Task.WaitAll(
        tasks
        |> Seq.map (fun it -> it :> Task)
        |> Seq.toArray
    )

    0 // return an integer exit code
