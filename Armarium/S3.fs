namespace Armarium

open System.IO
open FsToolbox.S3.Common


[<RequireQualifiedAccess>]
module S3 =

    module Internal =

        open FsToolbox.S3

        let getStream (ctx: S3Context) (bucket: string) (key: string) =
            ctx.ObjectToStream(bucket, key)
            |> Result.map id
            |> Result.mapError FileReadError.Failure

        let openReadOneTimeOp (readArgs: FileReadOperationArguments) (uri: string) =
            let cfg =
                ({ AccessKey = readArgs.Credentials.Username |> Option.defaultValue ""
                   SecretKey = readArgs.Credentials.Password |> Option.defaultValue ""
                   RegionalEndpoint = readArgs.GetValue "regional-endpoint"
                   ServiceUrl = readArgs.GetValue "service-url" }
                : S3Config)

            let ctx = new S3Context(cfg)

            getStream ctx "" ""

        let readAllBytesOneTimeOp (readArgs: FileReadOperationArguments) (uri: string) =
            let cfg =
                ({ AccessKey = readArgs.Credentials.Username |> Option.defaultValue ""
                   SecretKey = readArgs.Credentials.Password |> Option.defaultValue ""
                   RegionalEndpoint = readArgs.GetValue "regional-endpoint"
                   ServiceUrl = readArgs.GetValue "service-url" }
                : S3Config)

            let ctx = new S3Context(cfg)

            getStream ctx "" ""
            |> Result.map (fun s ->
                use ms = new MemoryStream()

                s.CopyTo(ms)

                ms.ToArray())

        let writeAllBytesOneTimeOp (writesArgs: FileWriteOperationArguments) (path: string) (data: byte array) =
            let cfg =
                ({ AccessKey = writesArgs.Credentials.Username |> Option.defaultValue ""
                   SecretKey = writesArgs.Credentials.Password |> Option.defaultValue ""
                   RegionalEndpoint = writesArgs.GetValue "regional-endpoint"
                   ServiceUrl = writesArgs.GetValue "service-url" }
                : S3Config)

            let ctx = new S3Context(cfg)
            use ms = new MemoryStream(data)

            match ctx.SaveStream("", "", ms) with
            | Ok status ->
                // TODO check
                Ok()
            | Error f -> FileWriteError.Failure f |> Error
            
        let openReadReusableOp (cfg: S3Config) (readArgs: FileReadOperationArguments) (uri: string) =
            let ctx = new S3Context(cfg)

            getStream ctx "" ""

        let readAllBytesReusableOp (cfg: S3Config) (readArgs: FileReadOperationArguments) (uri: string) =
            

            let ctx = new S3Context(cfg)

            getStream ctx "" ""
            |> Result.map (fun s ->
                use ms = new MemoryStream()

                s.CopyTo(ms)

                ms.ToArray())

        let writeAllBytesReusableOp (cfg: S3Config) (writesArgs: FileWriteOperationArguments) (path: string) (data: byte array) =
            let ctx = new S3Context(cfg)
            use ms = new MemoryStream(data)

            match ctx.SaveStream("", "", ms) with
            | Ok status ->
                // TODO check
                Ok()
            | Error f -> FileWriteError.Failure f |> Error

    let oneTimeFileHandler =
        ({ AllowAnonymousReadAccess = false
           AllowAnonymousWriteAccess = false
           ReadAllBytes = Internal.readAllBytesOneTimeOp
           OpenRead = Internal.openReadOneTimeOp
           WriteAllBytes = Internal.writeAllBytesOneTimeOp }
        : FileHandler)
        
    let createReadArgs (accessKey: string) () = ()
        
    let reusableFileHandler (cfg: S3Config) =
        ({ AllowAnonymousReadAccess = false
           AllowAnonymousWriteAccess = false
           ReadAllBytes = Internal.readAllBytesReusableOp cfg
           OpenRead = Internal.openReadReusableOp cfg
           WriteAllBytes = Internal.writeAllBytesReusableOp cfg }
        : FileHandler)

    