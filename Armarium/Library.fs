namespace Armarium

open System
open System.IO
open FsToolbox.Core.Results

[<RequireQualifiedAccess>]
type EncryptionType =
    | None
    | Aes

    static member All() =
        [ EncryptionType.None; EncryptionType.Aes ]

    static member Deserialize(value: string) =
        match value.ToLower() with
        | "none" -> Some EncryptionType.None
        | "aes" -> Some EncryptionType.Aes
        | _ -> Option.None

    member et.Serialize() =
        match et with
        | EncryptionType.None -> "none"
        | EncryptionType.Aes -> "aes"

[<RequireQualifiedAccess>]
type CompressionType =
    | None
    | Gzip

    static member All() =
        [ CompressionType.None; CompressionType.Gzip ]

    static member Deserialize(value: string) =
        match value.ToLower() with
        | "none" -> Some CompressionType.None
        | "gzip" -> Some CompressionType.Gzip
        | _ -> Option.None

    member ct.Serialize() =
        match ct with
        | CompressionType.None -> "none"
        | CompressionType.Gzip -> "gzip"

[<RequireQualifiedAccess>]
type FileType =
    | Pdf
    | Json
    | Text
    | Binary

    static member All() =
        [ FileType.Pdf; FileType.Json; FileType.Text; FileType.Binary ]

    static member AllDetails() =
        FileType.All() |> List.map (fun ft -> ft.GetDetails())

    static member Deserialize(value: string) =
        match value.ToLower() with
        | "pdf" -> Some FileType.Pdf
        | "json" -> Some FileType.Json
        | "text" -> Some FileType.Text
        | "binary" -> Some FileType.Binary
        | _ -> None

    member ft.Serialize() =
        match ft with
        | FileType.Pdf -> "pdf"
        | FileType.Json -> "json"
        | FileType.Text -> "text"
        | FileType.Binary -> "binary"

    member ft.GetDetails() =
        match ft with
        | FileType.Pdf ->
            { Name = "pdf"
              Extension = ".pdf"
              ContentType = "application/pdf" }
        | FileType.Json ->
            { Name = "json"
              Extension = ".json"
              ContentType = "application/json" }
        | FileType.Text ->
            { Name = "text"
              Extension = ".txt"
              ContentType = "text/plain" }
        | FileType.Binary ->
            { Name = "binary"
              Extension = ".bin"
              ContentType = "application/octet-stream" }

and FileTypeDetails =
    { Name: string
      Extension: string
      ContentType: string }

[<RequireQualifiedAccess>]
type FileReadError =
    | FileNotExists of SchemeName: string * Path: string
    | AnonymousReadAccessNotSupported of SchemeName: string
    | SchemeNotFound of SchemeName: string
    | UriError of Message: string
    | UnhandledException of exn

    member fre.ToFailure(?displayMessageOverride: string) =
        match fre with
        | FileNotExists(schemeName, path) ->
            ({ Message = $"File `{path}` not found in scheme `{schemeName}`."
               DisplayMessage = displayMessageOverride |> Option.defaultValue "File not found."
               Exception = None }
            : FailureResult)
        | AnonymousReadAccessNotSupported schemeName ->
            ({ Message = $"Scheme `{schemeName}` does not support anonymous reads."
               DisplayMessage = displayMessageOverride |> Option.defaultValue "File could not be read."
               Exception = None }
            : FailureResult)
        | SchemeNotFound schemeName ->
            ({ Message = $"Scheme `{schemeName}` not found."
               DisplayMessage = displayMessageOverride |> Option.defaultValue "File could not be read."
               Exception = None }
            : FailureResult)
        | UriError message ->
            ({ Message = $"Uri error: `{message}`."
               DisplayMessage = displayMessageOverride |> Option.defaultValue "File could not be read."
               Exception = None }
            : FailureResult)
        | UnhandledException exn ->
            ({ Message = $"Unhandled exception: `{exn.Message}`."
               DisplayMessage = displayMessageOverride |> Option.defaultValue "File could not be read."
               Exception = Some exn }
            : FailureResult)


[<RequireQualifiedAccess>]
module FileReadError =

    let toFailure (fre: FileReadError) = fre.ToFailure()

    let toActionResult (fre: FileReadError) =
        fre |> toFailure |> ActionResult.Failure

type FileWriteError =
    | FileAlreadyExists of SchemeName: string * Path: string
    | AnonymousWriteAccessNotSupported of SchemeName: string
    | SchemeNotFound of SchemeName: string
    | UriError of Message: string
    | UnhandledException of exn

    member fwe.ToFailure(?displayMessageOverride: string) =
        match fwe with
        | FileAlreadyExists(schemeName, path) ->
            ({ Message = $"File `{path}` already exists in scheme `{schemeName}`."
               DisplayMessage = displayMessageOverride |> Option.defaultValue "File could not be saved."
               Exception = None }
            : FailureResult)
        | AnonymousWriteAccessNotSupported schemeName ->
            ({ Message = $"Scheme `{schemeName}` does not support anonymous writes."
               DisplayMessage = displayMessageOverride |> Option.defaultValue "File could not be saved."
               Exception = None }
            : FailureResult)
        | SchemeNotFound schemeName ->
            ({ Message = $"Scheme `{schemeName}` not found."
               DisplayMessage = displayMessageOverride |> Option.defaultValue "File could not be saved."
               Exception = None }
            : FailureResult)
        | UriError message ->
            ({ Message = $"Uri error: `{message}`."
               DisplayMessage = displayMessageOverride |> Option.defaultValue "File could not be saved."
               Exception = None }
            : FailureResult)
        | UnhandledException exn ->
            ({ Message = $"Unhandled exception: `{exn.Message}`."
               DisplayMessage = displayMessageOverride |> Option.defaultValue "File could not be saved."
               Exception = Some exn }
            : FailureResult)

[<RequireQualifiedAccess>]
module FileWriteError =

    let toFailure (fwe: FileWriteError) = fwe.ToFailure()

    let toActionResult (fwe: FileWriteError) =
        fwe |> toFailure |> ActionResult.Failure

type AccessCredentials =
    { Username: string option
      Password: string option
      AccessToken: string option
      SubscriptionReference: string option }

    static member Blank =
        { Username = None
          Password = None
          AccessToken = None
          SubscriptionReference = None }

    member ac.IsAnonymous() =
        ac.Username.IsNone
        && ac.Password.IsNone
        && ac.AccessToken.IsNone
        && ac.SubscriptionReference.IsNone

type FileReadOperationArguments =
    { Credentials: AccessCredentials
      ExtraValues: Map<string, string> }

    static member Blank =
        { Credentials = AccessCredentials.Blank
          ExtraValues = Map.empty }

    member fra.TryGetValue(key) = fra.ExtraValues.TryFind key

    member fra.GetValue(key, ?defaultValue) =
        match fra.TryGetValue key, defaultValue with
        | Some v, _ -> v
        | _, Some v -> v
        | None, None -> failwith $"Key not found: {key}"

type FileWriteOperationArguments =
    { Credentials: AccessCredentials
      Overwrite: bool
      FileType: FileType
      Encryption: EncryptionType
      Compression: CompressionType
      ExtraValues: Map<string, string> }

    static member Blank =
        { Credentials = AccessCredentials.Blank
          Overwrite = false
          FileType = FileType.Binary
          Encryption = EncryptionType.None
          Compression = CompressionType.None
          ExtraValues = Map.empty }

    member fwa.TryGetValue(key) = fwa.ExtraValues.TryFind key

    member fwa.GetValue(key, ?defaultValue) =
        match fwa.TryGetValue key, defaultValue with
        | Some v, _ -> v
        | _, Some v -> v
        | None, None -> failwith $"Key not found: {key}"

type FileRepository =
    { Handlers: Map<string, FileHandler>
      FileSystemHandlerOverrider: FileHandler option }

    static member Default =
        { Handlers = Map.empty
          FileSystemHandlerOverrider = None }

    member fr.ReadAllBytes(uri: Uri, ?args: FileReadOperationArguments) =
        try
            let args = defaultArg args FileReadOperationArguments.Blank

            match uri.IsFile with
            | true ->
                fr.FileSystemHandlerOverrider
                |> Option.map (fun fso -> fso.ReadAllBytes args uri.LocalPath)
                |> Option.defaultWith (fun _ ->
                    match File.Exists uri.LocalPath with
                    | true -> File.ReadAllBytes uri.LocalPath |> Ok
                    | false -> FileReadError.FileNotExists(uri.Scheme, uri.LocalPath) |> Error)
            | false ->
                fr.Handlers.TryFind uri.Scheme
                |> Option.map (fun fh ->
                    match args.Credentials.IsAnonymous(), fh.AllowAnonymousReadAccess with
                    | false, _ -> fh.ReadAllBytes args uri.LocalPath
                    | true, true -> fh.ReadAllBytes args uri.LocalPath
                    | true, false -> FileReadError.AnonymousReadAccessNotSupported uri.Scheme |> Error)
                |> Option.defaultWith (fun _ -> FileReadError.SchemeNotFound uri.Scheme |> Error)
        with exn ->
            FileReadError.UnhandledException exn |> Error
        |> Result.mapError (fun e -> e.ToFailure())
        |> ActionResult.fromResult

    member fr.ReadAllBytes(path: string, ?args: FileReadOperationArguments) =
        match Uri.TryCreate(path, UriKind.RelativeOrAbsolute) with
        | true, uri ->
            match args with
            | Some a -> fr.ReadAllBytes(uri, a)
            | None -> fr.ReadAllBytes(uri)
        | false, _ ->
            FileReadError.UriError "Uri could not be parsed."
            |> FileReadError.toActionResult

    member fr.OpenRead(uri: Uri, ?args: FileReadOperationArguments) =
        try
            let args = defaultArg args FileReadOperationArguments.Blank

            match uri.IsFile with
            | true ->
                fr.FileSystemHandlerOverrider
                |> Option.map (fun fso -> fso.OpenRead args uri.LocalPath)
                |> Option.defaultWith (fun _ ->
                    match File.Exists uri.LocalPath with
                    | true -> File.OpenRead uri.LocalPath :> Stream |> Ok
                    | false -> FileReadError.FileNotExists(uri.Scheme, uri.LocalPath) |> Error)
            | false ->
                fr.Handlers.TryFind uri.Scheme
                |> Option.map (fun fh ->
                    match args.Credentials.IsAnonymous(), fh.AllowAnonymousReadAccess with
                    | false, _ -> fh.OpenRead args uri.LocalPath
                    | true, true -> fh.OpenRead args uri.LocalPath
                    | true, false -> FileReadError.AnonymousReadAccessNotSupported uri.Scheme |> Error)
                |> Option.defaultWith (fun _ -> FileReadError.SchemeNotFound uri.Scheme |> Error)
        with exn ->
            FileReadError.UnhandledException exn |> Error
        |> Result.mapError (fun e -> e.ToFailure())
        |> ActionResult.fromResult

    member fr.OpenRead(path: string, ?args: FileReadOperationArguments) =
        match Uri.TryCreate(path, UriKind.RelativeOrAbsolute) with
        | true, uri ->
            match args with
            | Some a -> fr.OpenRead(uri, a)
            | None -> fr.OpenRead(uri)
        | false, _ ->
            FileWriteError.UriError "Uri could not be parsed."
            |> FileWriteError.toActionResult

    member fr.WriteAllBytes(uri: Uri, bytes: byte array, ?args: FileWriteOperationArguments) =
        try
            let args = defaultArg args FileWriteOperationArguments.Blank

            match uri.IsFile with
            | true ->
                fr.FileSystemHandlerOverrider
                |> Option.map (fun fso -> fso.WriteAllBytes args uri.LocalPath bytes)
                |> Option.defaultWith (fun _ ->
                    match File.Exists uri.LocalPath, args.Overwrite with
                    | true, true
                    | false, _ -> File.WriteAllBytes(uri.LocalPath, bytes) |> Ok
                    | true, false -> FileWriteError.FileAlreadyExists(uri.Scheme, uri.LocalPath) |> Error)
            | false ->
                fr.Handlers.TryFind uri.Scheme
                |> Option.map (fun fh ->
                    match args.Credentials.IsAnonymous(), fh.AllowAnonymousWriteAccess with
                    | false, _ -> fh.WriteAllBytes args uri.LocalPath bytes
                    | true, true -> fh.WriteAllBytes args uri.LocalPath bytes
                    | true, false -> FileWriteError.AnonymousWriteAccessNotSupported uri.Scheme |> Error)
                |> Option.defaultWith (fun _ -> FileWriteError.SchemeNotFound uri.Scheme |> Error)
        with exn ->
            FileWriteError.UnhandledException exn |> Error
        |> Result.mapError (fun f -> f.ToFailure())
        |> ActionResult.fromResult

    member fr.WriteAllBytes(path: string, bytes: byte array, ?args: FileWriteOperationArguments) =
        match Uri.TryCreate(path, UriKind.RelativeOrAbsolute) with
        | true, uri ->
            match args with
            | Some a -> fr.WriteAllBytes(uri, bytes, a)
            | None -> fr.WriteAllBytes(uri, bytes)
        | false, _ ->
            FileWriteError.UriError "Uri could not be parsed."
            |> FileWriteError.toActionResult

and FileHandler =
    { AllowAnonymousReadAccess: bool
      AllowAnonymousWriteAccess: bool
      ReadAllBytes: FileReadOperationArguments -> string -> Result<byte array, FileReadError>
      OpenRead: FileReadOperationArguments -> string -> Result<Stream, FileReadError>
      WriteAllBytes: FileWriteOperationArguments -> string -> byte array -> Result<unit, FileWriteError> }
