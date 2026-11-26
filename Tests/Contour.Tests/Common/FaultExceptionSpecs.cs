using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.Common;

/// <summary>
/// Набор тестов обработки ошибочных сообщений.
/// </summary>
public class FaultExceptionSpecs
{
    /// <summary>
    /// Если создается объект ошибки
    /// </summary>
    [TestFixture]
    public class WhenCreateFaultException
    {
        /// <summary>
        /// Тогда в случе если нет внутренних исключений, должен создаться объект без внутренних объектов ошибки.
        /// </summary>
        [Test]
        public void IfExceptionWithoutInnerExceptionsShouldBeWithoutInnerExceptions()
        {
            var convertedException = new ArgumentException("Неверно задан параметр.");

            var sut = new FaultException(convertedException);

            sut.InnerExceptions.Should().BeEmpty("Список исключений дожен быть пустым.");
        }

        /// <summary>
        /// Тогда в случае, если есть внутренние ошибки, должен создаваться объект с внутренним объектом ошибки.
        /// </summary>
        [Test]
        public void IfExceptionWithInnerExceptionShouldContainInnerException()
        {
            try
            {
                this.GetType()
                    .GetMethod("ThrowException", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.InvokeMethod)
                    .Invoke(this, new object[] { });
            }
            catch (TargetInvocationException ex)
            {
                var sut = new FaultException(ex);

                sut.InnerExceptions.Should().NotBeEmpty("Должны быть внутренние исключения.");
                sut.InnerExceptions.Should().HaveCount(1, "Должно быть одно внутреннее исключение.");
            }
        }

        /// <summary>
        /// Тогда в случае, если ошибка представлена экземпляром класса <see cref="System.AggregateException"/>, объект ошибки должен содержать внутренние объекты ошибки.
        /// </summary>
        [Test]
        public void IfExceptionWIthAggregatedExceptionShouldContainException()
        {
            try
            {
                var task = new Task<int>(
                    () =>
                    {
                        this.ThrowException();
                        return 0;
                    });
                task.Start();
                task.Wait(10);
            }
            catch (AggregateException exception)
            {
                var sut = new FaultException(exception);

                sut.InnerExceptions.Should().NotBeEmpty("Должны быть внутренние исключения.");
                sut.InnerExceptions.Should().HaveCount(1, "Должно быть одно внутреннее исключение.");
            }
        }

        /// <summary>
        /// В случае, если ошибка представлена экземпляром класса <see cref="System.AggregateException"/>,
        /// и ошибка сожержит внутреннюю ошибку класса <see cref="System.AggregateException"/>, которая в свою очередь содержит ошибки,
        /// объект ошибки <see cref="FaultException"/> должен содержать все внутренние объекты ошибки.
        /// Также не должно быть падений при преобразовании ошибки в <see cref="FaultException"/>
        /// </summary>
        [Test]
        public void IfAggregatedExceptionContainsInnerAggregateExceptionsFaultExceptionShouldContainAllExceptions()
        {
            try
            {
                throw new AggregateException(new AggregateException(new TimeoutException()));
            }
            catch (AggregateException exception)
            {
                Action act = () => new FaultException(exception);
                act.Should().NotThrow();

                var sut = new FaultException(exception);
                sut.InnerExceptions.Should().NotBeEmpty("Должны быть внутренние исключения.");
                sut.InnerExceptions.Should().HaveCount(1, "Должно быть одно внутреннее исключение.");
                sut.InnerExceptions.First().InnerExceptions.Should().NotBeEmpty("Внутренне исключение должно содержать исключения.");
                sut.InnerExceptions.First().InnerExceptions.Should().HaveCount(1, "Внутренне исключение должно содержать одно внутреннее исключение.");
                sut.InnerExceptions.First().InnerExceptions.First().Type.Should().Be(exception.InnerExceptions.First().InnerException.GetType().FullName);
            }
        }

        private void ThrowException()
        {
            throw new NotImplementedException();
        }
    }
}