<?php

namespace Kubia\Camunda;

use Camunda\Entity\Request\ProcessInstanceRequest;
use Camunda\Service\ProcessInstanceService;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use Kubia\Logger\Logger;

/**
 * Abstract Class CamundaBaseConnector
 * @package Kubia\Camunda
 */
abstract class CamundaBaseConnector
{
    /** @var \PhpAmqpLib\Connection\AMQPStreamConnection */
    public $connection;

    /** @var \PhpAmqpLib\Channel\AMQPChannel */
    public $channel;

    /** @var \PhpAmqpLib\Message\AMQPMessage */
    public $msg;

    /** @var string */
    public $camundaUrl;

    /** @var object */
    public $processVariables;

    /** @var array */
    public $updatedVariables;

    /** @var array */
    public $message;

    /** @var array */
    public $headers;

    /** @var string */
    public $requestErrorMessage = 'Request error';

    /** @var array Unsafe parameters in headers **/
    public $unsafeHeadersParams = ['camundaBusinessKey'];

    /** @var string */
    public $logOwner = '';

    /** @var array */
    public $camundaConfig = [];

    /** @var array */
    public $rmqConfig = [];

    /**
     * @param AMQPMessage $msg
     * @return void
     */
    abstract protected function callback(AMQPMessage $msg): void;

    /**
     * CamundaBaseConnector constructor.
     * @param AMQPStreamConnection $connection
     * @param array $camundaConfig
     * @param array $rmqConfig
     */
    public function __construct(AMQPStreamConnection &$connection, array $camundaConfig, array $rmqConfig)
    {
        $this->camundaConfig = $camundaConfig;
        $this->rmqConfig = $rmqConfig;
        // connect to camunda api with basic auth
        $this->camundaUrl = sprintf($this->camundaConfig['apiUrl'], $this->camundaConfig['apiLogin'], $this->camundaConfig['apiPass']);
        $this->connection = $connection;
        $this->channel = $this->connection->channel();
    }

    /**
     * Get process variables
     * @return bool
     */
    public function getProcessVariables(): bool
    {
        $processInstanceId = $this->headers['camundaProcessInstanceId'];

        // Get process variables request
        $getVariablesRequest = (new ProcessInstanceRequest())
            ->set('deserializeValues', false);

        $getVariablesService = new ProcessInstanceService($this->camundaUrl);
        $this->processVariables = $getVariablesService->getVariableList($processInstanceId, $getVariablesRequest);

        if($getVariablesService->getResponseCode() != 200) {
            $this->processVariables = null;

            $logMessage = sprintf(
                "Process variables from process instance <%s> not received, because `%s`",
                $processInstanceId,
                $getVariablesService->getResponseContents()->message ?? $this->requestErrorMessage
            );
            $this->logError($logMessage);

            return false;
        } else {
            return true;
        }
    }

    /**
     * Validate message
     */
    public function validateMessage(): void
    {
        // Headers
        if(!$this->headers) {
            $logMessage = '`headers` not is set in incoming message';
            $this->logError($logMessage);
            exit(1);
        }

        // Unsafe params
        foreach ($this->unsafeHeadersParams as $paramName) {
            if(!isset($this->headers[$paramName])) {
                $logMessage = '`' . $paramName . '` param not is set in incoming message';
                $this->logError($logMessage);
                exit(1);
            }
        }
    }

    /**
     * if synchronous mode
     * add correlation id and temporary queue
     */
    function mixRabbitCorrelationInfo(): void
    {
        if($this->msg->has('correlation_id') && $this->msg->has('reply_to')) {
            $this->updatedVariables['rabbitCorrelationId'] = [
                'value' => $this->msg->get('correlation_id'),
                'type'  => 'string',
            ];
            $this->updatedVariables['rabbitCorrelationReplyTo'] = [
                'value' => $this->msg->get('reply_to'),
                'type'  => 'string',
            ];
        }
    }


    /**
     * Get formatted success response
     * for synchronous request
     * @param string $processInstanceId
     * @return string
     */
    public function getSuccessResponseForSynchronousRequest(string $processInstanceId): string
    {
        $response = [
            'success' => true,
            'camundaProcessInstanceId' => $processInstanceId
        ];

        return json_encode($response);
    }

    /**
     * Get formatted error response
     * for synchronous request
     * @param string $message
     * @return string
     */
    public function getErrorResponseForSynchronousRequest(string $message): string
    {
        $response = [
            'success' => false,
            'error'   => [
                [
                    'message' => $message
                ]
            ]
        ];

        return json_encode($response);
    }

    /**
     * Send synchronous response
     * @param AMQPMessage $msg
     * @param bool $success
     * @param string $processInstanceId
     */
    public function sendSynchronousResponse(AMQPMessage $msg, bool $success = false, string $processInstanceId = null): void
    {
        if($success)
            $responseToSync = $this->getSuccessResponseForSynchronousRequest($processInstanceId);
        else
            $responseToSync = $this->getErrorResponseForSynchronousRequest($this->requestErrorMessage);

        $sync_msg = new AMQPMessage($responseToSync, ['correlation_id' => $msg->get('correlation_id')]);
        $this->msg->delivery_info['channel']->basic_publish($sync_msg, '', $msg->get('reply_to'));
    }

    /**
     * Logging if system error
     * @param string $message
     */
    public function logError(string $message): void
    {
        Logger::stdout($message, 'input', $this->rmqConfig['queue'], $this->logOwner, 1);

        if(isset($this->rmqConfig['queueLog'])) {
            Logger::elastic('bpm',
                'started',
                'error',
                $this->data ?? (object)[],
                (object)[],
                ['type' => 'system', 'message' => $message],
                $this->channel,
                $this->rmqConfig['queueLog']
            );
        }
    }

    /**
     * Close connection
     */
    public function cleanupConnection(): void
    {
        // Connection might already be closed.
        // Ignoring exceptions.
        try {
            if($this->connection !== null) {
                $this->connection->close();
            }
        } catch (\ErrorException $e) {
        }
    }

    /**
     * Shutdown
     */
    public function shutdown(): void
    {
        $this->connection->close();
    }

    /**
     * Initialize and run in endless loop
     */
    public function run(): void
    {
        while(true) {
            try {
                register_shutdown_function([$this, 'shutdown']);

                Logger::stdout('Waiting for messages. To exit press CTRL+C', 'input', $this->rmqConfig['queue'], $this->logOwner, 0);

                $this->channel = $this->connection->channel();
                $this->channel->confirm_select(); // change channel mode to confirm mode
                $this->channel->basic_qos(0, 1, false); // one message in one loop
                $this->channel->basic_consume($this->rmqConfig['queue'], '', false, false, false, false, [$this, 'callback']);

                while ($this->channel->is_consuming()) {
                    $this->channel->wait(null, true, 0);
                    usleep($this->rmqConfig['tickTimeout']);
                }

            } catch(AMQPRuntimeException $e) {
                echo $e->getMessage() . PHP_EOL;
                $this->cleanupConnection();
                usleep($this->rmqConfig['reconnectTimeout']);
            } catch(\RuntimeException $e) {
                echo "Runtime exception " . $e->getMessage() . PHP_EOL;
                $this->cleanupConnection();
                usleep($this->rmqConfig['reconnectTimeout']);
            } catch(\ErrorException $e) {
                echo "Error exception " . $e->getMessage() . PHP_EOL;
                $this->cleanupConnection();
                usleep($this->rmqConfig['reconnectTimeout']);
            }
        }
    }
}