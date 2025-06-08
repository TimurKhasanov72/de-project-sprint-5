import requests
from typing import List, Dict


from examples.stg.models.Curier import CurierObj
from examples.stg.models.Delivery import DeliveryObj


class DeliveryServiceAPI:
    '''Апи сервиса доставки.'''

    def __init__(self, base_url:str, headers: Dict[str, str]):
        self._base_url = base_url
        self._headers = headers

    def list_curiers(
        self, 
        limit: int, 
        offset: int, 
        sort_field='name',
        sort_direction='asc'
    ) -> List[CurierObj]:
        '''Получаем список курьеров.'''

        params = {
            'limit': limit,
            'offset': offset,
            'sort_field': sort_field,
            'sort_direction': sort_direction
        }
        response = requests.get(
            f'{self._base_url}/couriers', 
            params=params, 
            headers=self._headers
        )

        if response.status_code != 200:
            raise Exception(f'{response.status_code}: ${response.text}')
        
        
        return [CurierObj(id=item.get('_id'), name=item.get('name')) for item in response.json()]
    
    def list_deliveries(
        self, 
        limit: int, 
        offset: int, 
        sort_field='name',
        sort_direction='asc'
    ) -> List[DeliveryObj]:
        '''Получаем список курьеров.'''

        params = {
            'limit': limit,
            'offset': offset,
            'sort_field': sort_field,
            'sort_direction': sort_direction
        }
        response = requests.get(
            f'{self._base_url}/deliveries', 
            params=params, 
            headers=self._headers
        )

        if response.status_code != 200:
            raise Exception(f'{response.status_code}: ${response.text}')
                
        return [
            DeliveryObj(
                delivery_id=item.get('delivery_id'),
                delivery_ts=item.get('delivery_ts'),
                payload={
                    "delivery_id": item.get('delivery_id'),
                    "order_id": item.get('order_id'),
                    "courier_id": item.get('courier_id'),
                    "rate": item.get('rate'),
                    "tip_sum": item.get('tip_sum')
                }
            ) for item in response.json()
        ]