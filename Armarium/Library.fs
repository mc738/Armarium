namespace Armarium

open System
open System.IO
open FsToolbox.Core
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
    // General
    | Binary
    | Text
    // Data
    | Json
    | Xml
    | Csv
    // Documents
    | Pdf
    // Web
    | Html
    | Css
    | JavaScript
    // Audio
    | Mp3
    | Wma
    | RealAudio
    | Wav
    // Images
    | Gif
    | Jpeg
    | Png
    | Tiff
    | Svg
    | WebP
    // Video
    | Mpeg
    | Mp4
    | QuickTime
    | Wmv
    | WebM


    static member All() =
        [ FileType.Binary
          FileType.Text
          FileType.Json
          FileType.Xml
          FileType.Csv
          FileType.Pdf
          FileType.Html
          FileType.Css
          FileType.JavaScript
          FileType.Mp3
          FileType.Wma
          FileType.RealAudio
          FileType.Wav
          FileType.Gif
          FileType.Jpeg
          FileType.Png
          FileType.Tiff
          FileType.Svg
          FileType.WebP
          FileType.Mpeg
          FileType.Mp4
          FileType.QuickTime
          FileType.Wmv
          FileType.WebM ]

    static member AllDetails() =
        FileType.All() |> List.map (fun ft -> ft.GetDetails())

    static member TryDeserialize(str: string) =
        match str.ToLower() with
        | "bin"
        | "exe"
        | "binary" -> Ok FileType.Binary
        | "text" -> Ok FileType.Text
        | "json" -> Ok FileType.Json
        | "xml" -> Ok FileType.Xml
        | "csv" -> Ok FileType.Csv
        | "pdf" -> Ok FileType.Pdf
        | "html" -> Ok FileType.Html
        | "css" -> Ok FileType.Css
        | "js"
        | "javascript" -> Ok FileType.JavaScript
        | "mp3" -> Ok FileType.Mp3
        | "wma" -> Ok FileType.Wma
        | "realaudio" -> Ok FileType.RealAudio
        | "wav" -> Ok FileType.Wav
        | "gif" -> Ok FileType.Gif
        | "jpeg" -> Ok FileType.Jpeg
        | "png" -> Ok FileType.Png
        | "tiff" -> Ok FileType.Tiff
        | "svg" -> Ok FileType.Svg
        | "webp" -> Ok FileType.WebP
        | "mpeg" -> Ok FileType.Mpeg
        | "mp4" -> Ok FileType.Mp4
        | "quicktime" -> Ok FileType.QuickTime
        | "wmv" -> Ok FileType.Wmv
        | "webm" -> Ok FileType.WebM
        | _ -> Error $"Unknown file type `{str}`"

    static member Deserialize(value: string) =
        match FileType.TryDeserialize value with
        | Ok ft -> Some ft
        | Error _ -> None

    member ft.Serialize() =
        match ft with
        | Binary -> "binary"
        | Text -> "text"
        | Json -> "json"
        | Xml -> "xml"
        | Csv -> "csv"
        | Pdf -> "pdf"
        | Html -> "html"
        | Css -> "css"
        | JavaScript -> "javascript"
        | Mp3 -> "mp3"
        | Wma -> "wma"
        | RealAudio -> "realaudio"
        | Wav -> "wav"
        | Gif -> "gif"
        | Jpeg -> "jpeg"
        | Png -> "png"
        | Tiff -> "tiff"
        | Svg -> "svg"
        | WebP -> "webp"
        | Mpeg -> "mpeg"
        | Mp4 -> "mp4"
        | QuickTime -> "quicktime"
        | Wmv -> "wmv"
        | WebM -> "webm"

    member ft.GetExtension() =
        match ft with
        | Binary -> ".bin"
        | Text -> ".txt"
        | Json -> ".json"
        | Xml -> ".xml"
        | Csv -> ".csv"
        | Pdf -> ".pdf"
        | Html -> ".html"
        | Css -> ".css"
        | JavaScript -> ".js"
        | Mp3 -> ".mp3"
        | Wma -> ".wma"
        | RealAudio -> "ra"
        | Wav -> ".wav"
        | Gif -> ".gif"
        | Jpeg -> ".jpeg"
        | Png -> ".png"
        | Tiff -> ".tiff"
        | Svg -> ".svg"
        | WebP -> ".webp"
        | Mpeg -> ".mpeg"
        | Mp4 -> ".mp4"
        | QuickTime -> ".quicktime"
        | Wmv -> ".wmv"
        | WebM -> ".webm"

    member ft.GetContentType() =
        match ft with
        | Binary -> "application/octet-stream"
        | Text -> "text/plain"
        | Json -> "application/json"
        | Xml -> "application/xml"
        | Csv -> "text/csv"
        | Pdf -> "application/pdf"
        | Html -> "text/html"
        | Css -> "text/css"
        | JavaScript -> "application/javascript"
        | Mp3 -> "audio/mpeg"
        | Wma -> "audio/x-ms-wma"
        | RealAudio -> "audio/vnd.rn-realaudio"
        | Wav -> "audio/x-wav"
        | Gif -> "image/gif"
        | Jpeg -> "image/jpeg"
        | Png -> "image/png"
        | Tiff -> "image/tiff"
        | Svg -> "image/svg+xml"
        | WebP -> "image/webp"
        | Mpeg -> "video/mpeg"
        | Mp4 -> "video/mp4"
        | QuickTime -> "video/quicktime"
        | Wmv -> "video/x-ms-wmv"
        | WebM -> "video/webm"

    member ft.GetDetails() =
        { Name = ft.Serialize()
          Extension = ft.GetExtension()
          ContentType = ft.GetContentType() }

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
    | Failure of FailureResult

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
        | Failure f -> f

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
    | Failure of FailureResult

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
        | Failure f -> f

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
                    | true ->
                        // TODO check local path is correct
                        File.OpenRead uri.LocalPath :> Stream |> Ok
                    | false -> FileReadError.FileNotExists(uri.Scheme, uri.LocalPath) |> Error)
            | false ->
                fr.Handlers.TryFind uri.Scheme
                |> Option.map (fun fh ->
                    match args.Credentials.IsAnonymous(), fh.AllowAnonymousReadAccess with
                    | false, _ ->
                        // TODO check local path is correct
                        fh.OpenRead args uri.LocalPath
                    | true, true ->
                        // TODO check local path is correct
                        fh.OpenRead args uri.LocalPath
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
                    | false, _ ->
                        // TODO check local path is correct
                        File.WriteAllBytes(uri.LocalPath, bytes) |> Ok
                    | true, false -> FileWriteError.FileAlreadyExists(uri.Scheme, uri.LocalPath) |> Error)
            | false ->
                fr.Handlers.TryFind uri.Scheme
                |> Option.map (fun fh ->
                    match args.Credentials.IsAnonymous(), fh.AllowAnonymousWriteAccess with
                    | false, _ ->
                        // TODO check local path is correct
                        fh.WriteAllBytes args uri.LocalPath bytes
                    | true, true ->
                        // TODO check local path is correct
                        fh.WriteAllBytes args uri.LocalPath bytes
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
