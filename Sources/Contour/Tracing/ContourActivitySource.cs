using System.Diagnostics;

namespace Contour.Tracing;

public static class ContourActivitySource
{
    public static readonly ActivitySource Source = new("Contour");
}