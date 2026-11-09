using System.Net.Sockets;
using AdminPanelAPI.QdrantSync.Services;
using Xunit;

namespace AdminPanelAPI.Tests;

public class SmtpDiagnosticsTests
{
    [Fact]
    public void Describe_IncludesInnerExceptionsAndSocketCode()
    {
        var ex = new InvalidOperationException("Failure sending mail.",
            new IOException("Unable to read data", new SocketException((int)SocketError.ConnectionReset)));

        var text = SmtpDiagnostics.Describe(ex);

        Assert.StartsWith("InvalidOperationException: Failure sending mail. -> IOException: Unable to read data -> SocketException(ConnectionReset)", text);
    }
}
