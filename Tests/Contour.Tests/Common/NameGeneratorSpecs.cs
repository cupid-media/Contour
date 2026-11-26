using Contour.Transport.RabbitMQ.Topology;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.Common;

/// <summary>
/// Набор тестов для генератора имен.
/// </summary>
public class NameGeneratorSpecs
{
    /// <summary>
    /// При генерации случайного имени.
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class WhenGenerateRandomName
    {
        /// <summary>
        /// Должен вернуть один символ.
        /// </summary>
        [Test]
        public void ShouldReturnOneSymbol()
        {
            var result = NameGenerator.GetRandomName(1);

            result.Should().HaveLength(1);
        }

        /// <summary>
        /// Должен вернуть пустую строку.
        /// </summary>
        [Test]
        public void ShouldReturnZeroSymbols()
        {
            var result = NameGenerator.GetRandomName(0);

            result.Should().HaveLength(0);
        }

        /// <summary>
        /// Должен 5 символов.
        /// </summary>
        [Test]
        public void ShouldReturnFiveSymbols()
        {
            var result = NameGenerator.GetRandomName(5);

            result.Should().HaveLength(5);
        }

        /// <summary>
        /// Должен 40 символов.
        /// </summary>
        [Test]
        public void ShouldReturnFortySymbols()
        {
            var result = NameGenerator.GetRandomName(40);

            result.Should().HaveLength(40);
        }
    }
}