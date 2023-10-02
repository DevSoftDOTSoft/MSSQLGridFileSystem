Imports System.Data.SqlClient
Imports System.IO
Imports System.IO.Compression
Imports System.Runtime.InteropServices.ComTypes
Imports System.Security.Cryptography

Module Module1

    Private WithEvents segmenter As MSSQLChunkSegmenter
    Private Function uf_connectDB(ByVal serverHost As String, ByVal sqlEngine As String, ByVal userName As String, ByVal passwd As String, ByVal dataBase As String) As SqlConnection
        Dim conString As String = $"Server={serverHost};uid={userName};pwd={passwd};Database={dataBase};MultipleActiveResultSets=true"
        Return New System.Data.SqlClient.SqlConnection(conString)
    End Function

    Sub Main()
        Dim mDocsSQLHost As String = "VMSQL\SQL2016"
        Dim mDocsSQLDB As String = "Docs"
        Dim mDocsSQLUser As String = "sa"
        Dim mDocsSQLPasswd As String = "123"
        Dim con As SqlConnection = uf_connectDB(mDocsSQLHost, "", mDocsSQLUser, mDocsSQLPasswd, mDocsSQLDB)

        segmenter = New MSSQLChunkSegmenter("mSegmentedFileData", con)

        Dim insertStamp As String = segmenter.insertFile("1.7z", -1)
        Console.WriteLine($"Done writing => {insertStamp}")

        Dim myFile As New FileInfo("1.7z")
        Dim sizeInBytes As Long = myFile.Length
        Dim readResp As Boolean = segmenter.readFile("Recovered1.7z", insertStamp, sizeInBytes)
        Console.WriteLine($"Done reading => {readResp}")

        Console.ReadKey()
    End Sub

    Private Sub segmenter_fileStatusChanged(status As Double) Handles segmenter.fileStatusChanged
        Console.WriteLine($"{status}%")
    End Sub
End Module


Public Class MSSQLChunkSegmenter

    Public Event fileStatusChanged(ByVal status As Double)
    Private insertTable As String
    Private mssqlConnection As SqlConnection
    Private key As String = "123456789"

    Public Sub New(ByVal tableName As String, ByVal sqlConnection As SqlConnection)
        Me.insertTable = tableName
        Me.mssqlConnection = sqlConnection
        tableCheck(tableName)
    End Sub
    Public Function insertFile(ByVal insertFileName As String, Optional ByVal insertBuffer As Long = 8388608) As String
        Dim tofResp As String = ""
        If insertBuffer = -1 Then
            Dim myFile As New FileInfo(insertFileName)
            insertBuffer = getBufferSizeByLen(myFile.Length)
        End If
        Using inStream As New System.IO.FileStream(insertFileName, FileMode.Open, FileAccess.Read)
            tofResp = bufferCopyHandlerFileInsert(inStream, insertBuffer)
        End Using
        Return tofResp
    End Function
    Public Function readFile(ByVal outputFileName As String, ByVal fileID As String, Optional ByVal fileSize As Long = 0) As Boolean
        Dim toContinue As Boolean = True
        Dim dataOrder As Integer = 0
        Dim currentBytesRead As Long = 0
        Dim tDES As System.Security.Cryptography.TripleDES = getTripleDES(key, CipherMode.CBC)
        Dim tDESDecrypter As System.Security.Cryptography.ICryptoTransform = tDES.CreateDecryptor
        ' ***
        Using outStream As New System.IO.FileStream(outputFileName, FileMode.Create, FileAccess.Write)
            While toContinue
                Dim fetchQuery As String = $"
                    Select [data] From {insertTable} (nolock) Where id = '{fileID}' And dataorder = {dataOrder}
                "
                Dim dtFetch As DataTable = uf_runQuery(fetchQuery, mssqlConnection)
                toContinue = IIf(dtFetch.Rows.Count > 0, True, False)
                If toContinue Then
                    ' * Read the Data to the Buffer!
                    Dim readData As Byte() = dtFetch.Rows(0)("data")
                    Dim readStream As New MemoryStream(readData)
                    Dim decompBuffer As Byte() = New Byte(4098) {}
                    Using cryptoStream As New CryptoStream(readStream, tDESDecrypter, CryptoStreamMode.Read)
                        Using Decomp As GZipStream = New GZipStream(cryptoStream, CompressionMode.Decompress)
                            Dim responseBuffer As Integer = 1
                            While responseBuffer <> 0
                                responseBuffer = Decomp.Read(decompBuffer, 0, decompBuffer.Length)
                                currentBytesRead += responseBuffer
                                outStream.Write(decompBuffer, 0, responseBuffer)
                                If fileSize > 0 Then
                                    RaiseEvent fileStatusChanged(getPercent(currentBytesRead, fileSize))
                                End If
                            End While
                        End Using
                    End Using
                    ' *** Increment at End ***
                    dataOrder += 1
                Else
                    If currentBytesRead = 0 Then
                        Return False
                    End If
                End If
            End While
        End Using
        Return True
    End Function
    Private Sub tableCheck(ByVal tableName As String)
        Dim queryTableExists As String = $"
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{tableName}]') AND type in (N'U'))
	            Select 0 as 'resp'
            ELSE
	            Select 1 as 'resp'
        "
        Dim dtTableExists As DataTable = uf_runQuery(queryTableExists, Me.mssqlConnection)
        Dim dtResp As Integer = dtTableExists.Rows(0)("resp")
        If dtResp = 0 Then
            Dim queryCreateTab As String = $"
                CREATE TABLE [dbo].[{tableName}](
	                [id] [nchar](25) NOT NULL,
	                [data] [varbinary](max) NOT NULL,
	                [dataorder] [int] NOT NULL
                ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
            "
            Dim createTabResp As Integer = uf_runNonQuery(queryCreateTab, Me.mssqlConnection)
        End If
    End Sub
    Private Function bufferCopyHandlerFileInsert(ByVal inputStream As Stream, ByVal bufferSize As Long) As String
        Dim fStamp As String = uf_generateUID(25)
        Dim insertOrder As Integer = 0
        Dim getter As Integer = bufferSize
        Dim inputFileSize As Long = inputStream.Length
        Dim currentBytesRead As Long = 0
        Dim tDES As System.Security.Cryptography.TripleDES = getTripleDES(key, CipherMode.CBC)
        Dim tDESEncrypter As System.Security.Cryptography.ICryptoTransform = tDES.CreateEncryptor
        While getter <> 0
            Dim buff(bufferSize) As Byte
            getter = inputStream.Read(buff, 0, buff.Length)
            currentBytesRead += getter
            Dim result As New MemoryStream()
            Using cryptoStream As New CryptoStream(result, tDESEncrypter, CryptoStreamMode.Write)
                Using Compress As GZipStream = New GZipStream(cryptoStream, CompressionLevel.Optimal)
                    Compress.Write(buff, 0, getter)
                End Using
            End Using
            Dim finalData As Byte() = result.ToArray()
            If finalData.Length > 0 Then
                Dim insertResp = insertBinaryData(insertTable, fStamp, finalData, insertOrder, Me.mssqlConnection)
                If insertResp <> 1 Then
                    ' * Delete all data with this ID
                    Dim deleteQuery As String = $"Delete from {insertTable} Where id = '{fStamp}' "
                    Dim delResp As Integer = uf_runNonQuery(deleteQuery, Me.mssqlConnection)
                    Return "False"
                End If
                RaiseEvent fileStatusChanged(getPercent(currentBytesRead, inputFileSize))
            End If
            ' *** Increment at End ***
            insertOrder += 1
        End While
        Return fStamp
    End Function
    Private Function uf_runNonQuery(ByVal queryString As String, cnn As System.Data.SqlClient.SqlConnection) As Integer
        If cnn.State <> ConnectionState.Open Then
            cnn.Open()
        End If
        Try
            Using cmd As SqlCommand = New SqlCommand(queryString, cnn)
                Return cmd.ExecuteNonQuery()
            End Using
        Catch ex As Exception
            Console.WriteLine(ex.ToString())
            Return -666
        End Try
    End Function
    Private Function getPercent(ByVal curr As Long, ByVal max As Long)
        Return (curr * 100) / max
    End Function
    Private Function uf_generateUID(Optional ByVal maxDigits As Integer = 32) As String
        Return Microsoft.VisualBasic.Left(Guid.NewGuid().ToString().Replace("-", ""), maxDigits)
    End Function
    Public Function insertBinaryData(ByVal dataTable As String, ByVal ficheiroStamp As String,
                                     ByVal data As Byte(), ByVal order As Integer,
                                     ByVal con As SqlConnection) As Integer
        If con.State <> ConnectionState.Open Then
            con.Open()
        End If
        Using cm As SqlCommand = con.CreateCommand()
            Dim queryFileData As String = $"
                    INSERT INTO [dbo].[{dataTable}]
                                (
                                     [id]
                                    ,[dataorder]
                                    ,[data]
                                )
                    VALUES      (@stamp, @order, @data)
                "
            cm.CommandText = queryFileData
            cm.Parameters.AddWithValue("@stamp", ficheiroStamp)
            cm.Parameters.AddWithValue("@order", order)
            cm.Parameters.AddWithValue("@data", data)
            Return cm.ExecuteNonQuery()
        End Using
    End Function
    Private Function uf_runQuery(ByVal queryString As String, ByVal cnn As System.Data.SqlClient.SqlConnection) As DataTable
        Dim dad As New System.Data.SqlClient.SqlDataAdapter(queryString, cnn)
        Dim dtbtmp As New DataTable
        dad.Fill(dtbtmp)
        dad.Dispose()
        Return dtbtmp
    End Function
    Private Function getTripleDES(ByVal key As String, Optional ByVal cipherMode As CipherMode = CipherMode.CBC) As System.Security.Cryptography.TripleDES
        Dim TripleDES As System.Security.Cryptography.TripleDES = System.Security.Cryptography.TripleDES.Create()
        TripleDES.Key = GetMD5Hash(key)
        TripleDES.IV = New Byte() {1, 2, 3, 4, 5, 6, 7, 8}
        TripleDES.Mode = cipherMode
        TripleDES.Padding = PaddingMode.ANSIX923
        Return TripleDES
    End Function
    Private Function GetMD5Hash(strToHash As String) As Byte()
        Dim md5Obj As New System.Security.Cryptography.MD5CryptoServiceProvider
        Dim bytesToHash() As Byte = System.Text.Encoding.ASCII.GetBytes(strToHash)
        bytesToHash = md5Obj.ComputeHash(bytesToHash)
        Return bytesToHash
    End Function
    Private Function GetSHA256Hash(strToHash As String) As Byte()
        Dim SHA256Obj As New System.Security.Cryptography.SHA256Cng
        Dim bytesToHash() As Byte = System.Text.Encoding.ASCII.GetBytes(strToHash)
        bytesToHash = SHA256Obj.ComputeHash(bytesToHash)
        Return bytesToHash
    End Function
    Private Function getBufferSizeByLen(ByVal len As Long)
        Dim oMegaByte As Long = 1048576
        Select Case len
            Case 0 To oMegaByte
                Return oMegaByte
            Case oMegaByte To (oMegaByte * 100)
                Return oMegaByte * 2
            Case (oMegaByte * 100) To (oMegaByte * 1000)
                Return oMegaByte * 3
            Case (oMegaByte * 1000) To (oMegaByte * 10000)
                Return oMegaByte * 5
            Case (oMegaByte * 10000) To (oMegaByte * 100000)
                Return oMegaByte * 8
            Case Else
                Return oMegaByte * 15
        End Select
    End Function
End Class