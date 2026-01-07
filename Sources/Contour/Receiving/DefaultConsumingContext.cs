using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Contour.Tracing;

namespace Contour.Receiving
{
    /// <summary>
    /// Контекст обработки полученного сообщения.
    /// </summary>
    /// <typeparam name="T">Тип полученного сообщения.</typeparam>
    internal class DefaultConsumingContext<T> : IConsumingContext<T>, IDeliveryContext
        where T : class
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultConsumingContext{T}"/> class. 
        /// </summary>
        /// <param name="busContext">
        /// The bus Context.
        /// </param>
        /// <param name="message">
        /// Входящее сообщение.
        /// </param>
        /// <param name="delivery">
        /// Полученное сообщение.
        /// </param>
        public DefaultConsumingContext(IBusContext busContext, Message<T> message, IDelivery delivery)
        {
            Message = message;
            Delivery = delivery;
            Bus = busContext;
        }

        /// <summary>
        /// Конечная точка, в которой зарегистрирован текущий обработчик сообщения.
        /// </summary>
        public IBusContext Bus { get; private set; }

        /// <summary>
        /// Возвращает доставленное сообщение.
        /// </summary>
        public IDelivery Delivery { get; private set; }

        /// <summary>
        /// Полученное сообщение.
        /// </summary>
        public Message<T> Message { get; private set; }

        /// <summary>
        /// Верно, если можно ответить на это сообщение.
        /// </summary>
        public bool CanReply => Delivery.CanReply;

        /// <summary>
        /// Помечает сообщение как обработанное.
        /// </summary>
        public void Accept()
        {
            Delivery.Accept();
        }

        /// <summary>
        /// Пересылает сообщение, устанавливая указанную метку.
        /// </summary>
        /// <param name="label">Новая метка, с которой пересылается сообщение.</param>
        public void Forward(MessageLabel label)
        {
            Forward(label, Message.Payload);
        }

        /// <summary>
        /// Пересылает сообщение, устанавливая указанную метку.
        /// </summary>
        /// <param name="label">Новая метка, с которой пересылается сообщение.</param>
        [Obsolete("Используйте ForwardAsync")]
        public void Forward(string label)
        {
            Forward(label, Message.Payload);
        }

        public async Task ForwardAsync(string label)
        {
            await ForwardWithActivityAsync(label.ToMessageLabel(), Message.Payload);
        }

        public async Task ForwardAsync<TOut>(string label, TOut payload) where TOut : class
        {
            await ForwardWithActivityAsync(label.ToMessageLabel(), payload);
        }

        /// <summary>
        /// Пересылает сообщение, устанавливая указанную метку.
        /// </summary>
        /// <param name="label">Новая метка, с которой пересылается сообщение.</param>
        /// <param name="payload">Новое содержимое сообщения.</param>
        /// <typeparam name="TOut">Тип сообщения.</typeparam>
        public void Forward<TOut>(MessageLabel label, TOut payload = default(TOut)) where TOut : class
        {
            ForwardWithActivity(label, payload);
        }

        /// <summary>
        /// Пересылает сообщение, устанавливая указанную метку.
        /// </summary>
        /// <param name="label">Новая метка, с которой пересылается сообщение.</param>
        /// <param name="payload">Новое содержимое сообщения.</param>
        /// <typeparam name="TOut">Тип сообщения.</typeparam>
        public void Forward<TOut>(string label, TOut payload = default(TOut)) where TOut : class
        {
            Forward(label.ToMessageLabel(), payload);
        }

        /// <summary>
        /// Forwards a message with proper Activity management (synchronous)
        /// </summary>
        private void ForwardWithActivity<TOut>(MessageLabel label, TOut payload) where TOut : class
        {
            try
            {
                Delivery.Forward(label, payload);
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// Forwards a message with proper Activity management (asynchronous)
        /// </summary>
        private async Task ForwardWithActivityAsync<TOut>(MessageLabel label, TOut payload) where TOut : class
        {
            try
            {
                await Delivery.Forward(label, payload);
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// Отправляет ответное сообщение, используя переданный в исходном
        /// сообщении обратный адрес и идентификатор сообщения.
        /// </summary>
        /// <typeparam name="TResponse">.NET тип отправляемого сообщения.</typeparam>
        /// <param name="response">Отправляемое сообщение.</param>
        /// <param name="expires">Настройки, которые определяют время пока ответ актуален.</param>
        public void Reply<TResponse>(TResponse response, Expires expires = null) where TResponse : class
        {
            if (!Delivery.CanReply)
            {
                return;
            }
            
            var replyHeaders = new Dictionary<string, object>();

            if (expires != null)
            {
                replyHeaders[Headers.Expires] = expires.ToString();
            }
            
            var replyMessage = new Message<TResponse>(MessageLabel.Empty, replyHeaders, response);
            
            try
            {
                W3CTraceContextProvider.InjectTraceContext(replyHeaders, Message);

                Delivery.ReplyWith(replyMessage);
            }
            catch
            {
                throw;
            }
        }
        
        /// <summary>
        /// Помечает сообщение как необработанное.
        /// </summary>
        /// <param name="requeue">
        /// Сообщение требуется вернуть во входящую очередь для повторной обработки.
        /// </param>
        public void Reject(bool requeue)
        {
            Delivery.Reject(requeue);
        }
    }
}
