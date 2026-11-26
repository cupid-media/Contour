using System;
using Confluent.Kafka;
using Contour.Transport.Kafka.Configuration;

namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Фабрика низкоуровневых Confluent-продюсеров.
///   Точка подмены транспорта в тестах.
/// </summary>
internal interface IKafkaProducerFactory
{
    /// <param name="options">Настройки отправителя.</param>
    /// <param name="errorHandler">
    ///   Колбэк ошибки продюсера: используется, чтобы пометить продюсер как нездоровый
    ///   при фатальной ошибке транспорта.
    /// </param>
    IProducer<string, byte[]> Create(KafkaSenderOptions options, Action<Error> errorHandler);
}
