using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Contour.Tests.Common;

/// <summary>
/// Тесты для проверки хранилища заголовков входящего сообщения.
/// </summary>
public class MessageHeaderStorageSpecs
{
    /// <summary>
    /// При сохранении заголовков.
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class WhenStoreHeaders
    {
        /// <summary>
        /// Должны исключаться заголовки из черного списка.
        /// </summary>
        [Test]
        public void ShouldFilterHeadersOfBlackList()
        {
            var excludedList = new List<string> { "b", "c" };

            var excludedListAddition = new List<string> { "e", "f" };

            var sut = new MessageHeaderStorage(excludedList);

            sut.RegisterExcludedHeaders(excludedListAddition);

            sut.Store(new Dictionary<string, object> { { "a", 1 }, { "b", 2 }, { "c", 3 }, { "d", 4 }, { "e", 5 }, { "f", 6 }, { "g", 7 } });

            var storedResult = sut.Load();

            CollectionAssert.DoesNotContain(storedResult.Keys, "b", "Заголовок должен быть исключен при сохранении.");
            CollectionAssert.DoesNotContain(storedResult.Keys, "c", "Заголовок должен быть исключен при сохранении.");
            CollectionAssert.DoesNotContain(storedResult.Keys, "e", "Заголовок должен быть исключен при сохранении.");
            CollectionAssert.DoesNotContain(storedResult.Keys, "f", "Заголовок должен быть исключен при сохранении.");
        }

        /// <summary>
        /// Звёздочка в черном списке должна исключать все заголовки.
        /// </summary>
        [Test]
        public void ShouldExcludeAllHeadersWhenAsteriskIsListed()
        {
            var sut = new MessageHeaderStorage(new List<string> { MessageHeaderStorage.AllHeaders });

            sut.Store(new Dictionary<string, object> { { "a", 1 }, { "b", 2 }, { Headers.CorrelationId, "3" } });

            CollectionAssert.IsEmpty(sut.Load(), "Ни один заголовок не должен переноситься дальше.");
        }

        /// <summary>
        /// Звёздочка, добавленная после создания хранилища, тоже должна исключать все заголовки.
        /// </summary>
        [Test]
        public void ShouldExcludeAllHeadersWhenAsteriskIsRegisteredLater()
        {
            var sut = new MessageHeaderStorage(new List<string> { "b" });

            sut.RegisterExcludedHeaders(new List<string> { MessageHeaderStorage.AllHeaders });

            sut.Store(new Dictionary<string, object> { { "a", 1 }, { "b", 2 } });

            CollectionAssert.IsEmpty(sut.Load(), "Ни один заголовок не должен переноситься дальше.");
        }
    }
}