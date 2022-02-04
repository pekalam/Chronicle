using System;
using System.Threading.Tasks;
using Chronicle.Persistence;

namespace Chronicle.Managers
{
    internal sealed class SagaProcessor : ISagaProcessor
    {
        private readonly ISagaStateRepository _repository;
        private readonly ISagaLog _log;

        public SagaProcessor(ISagaStateRepository repository, ISagaLog log)
        {
            _repository = repository;
            _log = log;
        }

        public async Task ProcessAsync<TMessage>(ISaga saga, TMessage message, ISagaState state,
            ISagaContext context) where TMessage : class
        {
            var action = (ISagaAction<TMessage>)saga;

            try
            {
                await action.HandleAsync(message, context);
            }
            catch (Exception ex)
            {
                context.SagaContextError = new SagaContextError(ex);

                if (!(saga.State is SagaStates.Rejected))
                {
                    saga.Reject(ex);
                }
            }
            finally
            {
                await UpdateSagaAsync(message, saga, state);
            }
        }

        private async Task UpdateSagaAsync<TMessage>(TMessage message, ISaga saga, ISagaState state)
            where TMessage : class
        {
            var sagaType = saga.GetType();

            var updatedSagaData = sagaType.GetProperty(nameof(ISaga<object>.Data))?.GetValue(saga);

            state.Update(saga.State, updatedSagaData);
            var logData = SagaLogData.Create(saga.Id, sagaType, message);

            await _repository.WriteAsync(state).ConfigureAwait(false);
            await _log.WriteAsync(logData).ConfigureAwait(false);
        }
    }
}
